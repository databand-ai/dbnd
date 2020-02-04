import logging
import signal
import time
import typing

from collections import defaultdict

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.constants import SystemTaskName, TaskRunState
from dbnd._core.errors import DatabandConfigError, friendly_error, show_error_once
from dbnd._core.errors.base import DatabandSigTermError
from dbnd._core.log.logging_utils import TaskContextFilter
from dbnd._core.plugin.dbnd_plugins import pm
from dbnd._core.task_build.task_context import TaskContextPhase, task_context
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.utils import json_utils
from dbnd._core.utils.basics.safe_signal import safe_signal
from dbnd._core.utils.seven import contextlib
from dbnd._core.utils.timezone import utcnow
from dbnd._core.utils.traversing import flatten, traverse_to_str


if typing.TYPE_CHECKING:
    from dbnd import Task

# Time to sleep while waiting for eventual consistency to finish.
EVENTUAL_CONSISTENCY_SLEEP_INTERVAL = 1

# Maximum number of sleeps for eventual consistency.
EVENTUAL_CONSISTENCY_MAX_SLEEPS = 30

logger = logging.getLogger(__name__)


class TaskRunRunner(TaskRunCtrl):
    @contextlib.contextmanager
    def task_run_execution_context(self):
        ctx_managers = [
            task_context(self.task_run.task, TaskContextPhase.RUN),
            TaskContextFilter.task_context(self.task.task_id),
            self.task_run.log.capture_task_log(),
            config.config_layer_context(self.task.task_meta.config_layer),
        ]

        ctx_managers.append(self.task_run.log.capture_stderr_stdout())

        ctx_managers += pm.hook.dbnd_task_run_context(task_run=self.task_run)
        with contextlib.ExitStack() as stack:
            for mgr in ctx_managers:
                stack.enter_context(mgr)
            yield

    @contextlib.contextmanager
    def task_run_driver_context(self):
        # we don't want logs/user wrappers at this stage
        ctx_managers = [
            task_context(self.task_run.task, TaskContextPhase.RUN),
            config.config_layer_context(self.task.task_meta.config_layer),
        ]
        with contextlib.ExitStack() as stack:
            for mgr in ctx_managers:
                stack.enter_context(mgr)
            yield

    def execute(self, airflow_context=None):
        task_run = self.task_run
        task = self.task  # type: Task
        task_run.airflow_context = airflow_context

        with self.task_run_execution_context():
            if task_run.run.is_killed():
                raise friendly_error.task_execution.databand_context_killed(
                    "task.execute_start of %s" % task
                )
            original_sigterm_signal = None
            try:

                def signal_handler(signum, frame):
                    logger.warning(
                        "Received SIGTERM. Raising  DatabandSigTermError() exception!"
                    )
                    raise DatabandSigTermError(
                        "Task received SIGTERM signal",
                        help_msg="Probably the job was canceled",
                    )

                original_sigterm_signal = safe_signal(signal.SIGTERM, signal_handler)

                task_run.start_time = utcnow()
                self.task_env.prepare_env()
                if task._complete():
                    task_run.set_task_reused()
                    return

                if not self.ctrl.should_run():
                    missing = find_non_completed(self.relations.task_outputs_user)
                    missing_str = non_completed_outputs_to_str(missing)
                    raise DatabandConfigError(
                        "You are missing some input tasks in your pipeline! \n\t%s\n"
                        "The task execution was disabled for '%s'."
                        % (missing_str, self.task_id)
                    )

                missing = []
                for partial_output in flatten(self.relations.task_inputs_user):
                    if not partial_output.exists():
                        missing.append(partial_output)
                if missing:
                    raise friendly_error.task_data_source_not_exists(
                        self, missing, downstream=[self.task]
                    )

                task_run.set_task_run_state(state=TaskRunState.RUNNING)
                try:
                    if task._should_resubmit(task_run):
                        args = task_run.task_engine.dbnd_executable + [
                            "execute",
                            "--dbnd-run",
                            str(task_run.run.driver_dump),
                            "task_submit",
                            "--task-id",
                            task_run.task.task_id,
                        ]
                        submit_task = self.task_run.task_engine.submit_to_engine_task(
                            env=task.task_env,
                            task_name=SystemTaskName.task_submit,
                            args=args,
                        )
                        task_run.run.run_dynamic_task(submit_task)
                        result = None
                    else:
                        result = self.task._task_submit()
                    self._save_task_band()
                    self.validate_complete()
                finally:
                    self.task_run.finished_time = utcnow()

                task_run.set_task_run_state(TaskRunState.SUCCESS)
                task_run.run.cleanup_after_task_run(task)

                return result
            except DatabandSigTermError as ex:
                logger.error(
                    "Sig TERM! Killing the task '%s' via task.on_kill()",
                    task_run.task.task_id,
                )
                error = TaskRunError.buid_from_ex(ex, task_run)
                try:
                    task.on_kill()
                except Exception:
                    logger.exception("Failed to kill task on user keyboard interrupt")
                task_run.set_task_run_state(TaskRunState.CANCELLED, error=error)
                raise
            except KeyboardInterrupt as ex:
                logger.error(
                    "User Interrupt! Killing the task %s", task_run.task.task_id
                )
                error = TaskRunError.buid_from_ex(ex, task_run)
                try:
                    if task._conf_confirm_on_kill_msg:
                        from dbnd._vendor import click

                        if click.confirm(task._conf_confirm_on_kill_msg, default=True):
                            task.on_kill()
                        else:
                            logger.warning(
                                "Task is not killed accordingly to user input!"
                            )
                    else:
                        task.on_kill()
                except Exception:
                    logger.exception("Failed to kill task on user keyboard interrupt")
                task_run.set_task_run_state(TaskRunState.CANCELLED, error=error)
                raise
            except SystemExit as ex:
                error = TaskRunError.buid_from_ex(ex, task_run)
                task_run.set_task_run_state(TaskRunState.CANCELLED, error=error)
                raise friendly_error.task_execution.system_exit_at_task_run(task, ex)
            except Exception as ex:
                error = TaskRunError.buid_from_ex(ex, task_run)
                task_run.set_task_run_state(TaskRunState.FAILED, error=error)
                show_error_once.set_shown(ex)
                raise
            finally:
                task_run.airflow_context = None
                if original_sigterm_signal:
                    safe_signal(signal.SIGTERM, original_sigterm_signal)

    def _save_task_band(self):
        if self.task.task_band:
            task_outputs = traverse_to_str(self.task.task_outputs)
            self.task.task_band.as_object.write_json(task_outputs)

    def validate_complete(self):
        if self.task._complete():
            return

        if self.wait_for_consistency():
            return

        missing = find_non_completed(self.relations.task_outputs_user)
        if not missing:
            raise friendly_error.task_has_not_complete_but_all_outputs_exists(self)

        missing_str = non_completed_outputs_to_str(missing)
        raise friendly_error.task_has_missing_outputs_after_execution(
            self.task, missing_str
        )

    def wait_for_consistency(self):
        for attempt in range(EVENTUAL_CONSISTENCY_MAX_SLEEPS):
            missing = find_non_completed(self.task.task_outputs)
            if not missing:
                return True
            missing_and_not_consistent = find_non_consistent(missing)
            if not missing_and_not_consistent:
                return False

            missing_str = non_completed_outputs_to_str(missing_and_not_consistent)
            logging.warning(
                "Some outputs are missing, potentially due to eventual consistency of "
                "your data store. Waining %s second to retry. Additional attempts: %s\n\t%s"
                % (
                    str(EVENTUAL_CONSISTENCY_SLEEP_INTERVAL),
                    str(EVENTUAL_CONSISTENCY_MAX_SLEEPS - attempt),
                    missing_str,
                )
            )

            time.sleep(EVENTUAL_CONSISTENCY_SLEEP_INTERVAL)
        return False


def find_non_completed(targets):
    missing = defaultdict(list)
    for k, v in targets.items():
        for partial_output in flatten(v):
            if not partial_output.exists():
                missing[k].append(partial_output)

    return missing


def find_non_consistent(targets):
    non_consistent = list()
    for k, v in targets.items():
        for partial_output in flatten(v):
            if not partial_output.exist_after_write_consistent():
                non_consistent.append(partial_output)

    return non_consistent


def non_completed_outputs_to_str(non_completed_outputs):
    return json_utils.dumps(non_completed_outputs)
