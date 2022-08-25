# © Copyright Databand.ai, an IBM Company 2022

import logging
import os
import signal
import typing
import webbrowser

from dbnd._core.constants import SystemTaskName, TaskRunState
from dbnd._core.current import try_get_current_task_run
from dbnd._core.errors import friendly_error, show_error_once
from dbnd._core.errors.base import DatabandSigTermError
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled, pm
from dbnd._core.task_build.task_context import TaskContextPhase
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.tracking.dbnd_spark_init import jvm_context_manager
from dbnd._core.utils import seven
from dbnd._core.utils.basics.nested_context import nested
from dbnd._core.utils.basics.signal_utils import safe_signal
from dbnd._core.utils.seven import contextlib
from dbnd._core.utils.timezone import utcnow


if typing.TYPE_CHECKING:
    from dbnd import Task

logger = logging.getLogger(__name__)


class TaskRunRunner(TaskRunCtrl):
    @contextlib.contextmanager
    def task_run_execution_context(self, handle_sigterm=True, capture_log=True):
        parent_task = try_get_current_task_run()
        current_task = self.task_run
        ctx_managers = [self.task.ctrl.task_context(phase=TaskContextPhase.RUN)]

        if capture_log:
            ctx_managers.append(self.task_run.log.capture_task_log())

        if handle_sigterm:
            ctx_managers.append(handle_sigterm_at_dbnd_task_run())

        ctx_managers.extend(pm.hook.dbnd_task_run_context(task_run=self.task_run))
        ctx_managers.append(jvm_context_manager(parent_task, current_task))

        with nested(*ctx_managers):
            yield

    def execute(self, airflow_context=None, allow_resubmit=True, handle_sigterm=True):
        self.task_run.airflow_context = airflow_context
        if airflow_context:
            # In the case the airflow_context has a different try_number than our task_run's attempt_number,
            # we need to update our task_run attempt accordingly.
            self.task_run.update_task_run_attempt(airflow_context["ti"].try_number)

        task_run = self.task_run
        run = task_run.run
        run_executor = run.run_executor
        run_config = run_executor.run_config
        task = self.task  # type: Task
        task_engine = task_run.task_engine
        if allow_resubmit and task_engine._should_wrap_with_submit_task(task_run):
            args = task_engine.dbnd_executable + [
                "execute",
                "--dbnd-run",
                str(run_executor.driver_dump),
                "task_execute",
                "--task-id",
                task_run.task.task_id,
            ]
            submit_task = self.task_run.task_engine.submit_to_engine_task(
                env=task.task_env, task_name=SystemTaskName.task_submit, args=args
            )
            submit_task.descendants.add_child(task.task_id)
            if run_config.open_web_tracker_in_browser:
                webbrowser.open_new_tab(task_run.task_tracker_url)
            run_executor.run_task_at_execution_time(submit_task)
            return

        with self.task_run_execution_context(handle_sigterm=handle_sigterm):
            if run.is_killed():
                raise friendly_error.task_execution.databand_context_killed(
                    "task.execute_start of %s" % task
                )
            try:
                self.task_env.prepare_env()
                if run_config.skip_completed_on_run and task._complete():
                    task_run.set_task_reused()
                    return
                task_run.set_task_run_state(state=TaskRunState.RUNNING)

                if not self.task.ctrl.should_run():
                    self.task.ctrl.validator.find_and_raise_missing_inputs()

                if run_config.validate_task_inputs:
                    self.ctrl.validator.validate_task_inputs()

                try:
                    result = self.task._task_submit()
                    self.ctrl.save_task_band()
                    if run_config.validate_task_outputs:
                        self.ctrl.validator.validate_task_is_complete()
                finally:
                    self.task_run.finished_time = utcnow()

                task_run.set_task_run_state(TaskRunState.SUCCESS)
                run.cleanup_after_task_run(task)

                return result
            except DatabandSigTermError as ex:
                logger.error(
                    "Sig TERM! Killing the task '%s' via task.on_kill()",
                    task_run.task.task_id,
                )
                run.run_executor._internal_kill()

                error = TaskRunError.build_from_ex(ex, task_run)
                try:
                    task.on_kill()
                except Exception:
                    logger.exception("Failed to kill task on user keyboard interrupt")
                task_run.set_task_run_state(TaskRunState.CANCELLED, error=error)
                raise
            except KeyboardInterrupt as ex:
                logger.warning(
                    "User Interrupt! Killing the task %s", task_run.task.task_id
                )
                error = TaskRunError.build_from_ex(ex, task_run)
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
                run.run_executor._internal_kill()
                raise
            except SystemExit as ex:
                error = TaskRunError.build_from_ex(ex, task_run)
                task_run.set_task_run_state(TaskRunState.CANCELLED, error=error)
                raise friendly_error.task_execution.system_exit_at_task_run(task, ex)
            except Exception as ex:
                error = TaskRunError.build_from_ex(ex, task_run)
                task_run.set_task_run_state(TaskRunState.FAILED, error=error)
                show_error_once.set_shown(ex)
                raise
            finally:
                task_run.airflow_context = None


@seven.contextlib.contextmanager
def handle_sigterm_at_dbnd_task_run():
    def signal_handler(signum, frame):
        logger.info("Task runner received signal. PID: %s. Exiting...", os.getpid())
        if is_plugin_enabled("dbnd-docker") and is_plugin_enabled("dbnd-airflow"):
            from dbnd_docker.kubernetes.kubernetes_engine_config import (
                ENV_DBND_POD_NAME,
            )

            if ENV_DBND_POD_NAME in os.environ:
                # We are running inside cluster
                # We should log all events on sigterm for debugging when running inside cluster
                from dbnd_airflow_contrib.kubernetes_metrics_logger import (
                    log_pod_events_on_sigterm,
                )

                log_pod_events_on_sigterm(frame)

        raise DatabandSigTermError(
            "Task received signal", help_msg="Probably the job was canceled"
        )

    original_sigterm_signal = None
    try:
        original_sigterm_signal = safe_signal(signal.SIGTERM, signal_handler)
        yield
    finally:
        if original_sigterm_signal:
            safe_signal(signal.SIGTERM, original_sigterm_signal)
