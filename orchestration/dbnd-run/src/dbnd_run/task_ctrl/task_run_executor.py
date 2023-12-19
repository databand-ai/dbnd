# © Copyright Databand.ai, an IBM Company 2022

import logging
import os
import signal
import typing
import webbrowser

from dbnd._core.constants import SystemTaskName, TaskRunState
from dbnd._core.errors import show_error_once
from dbnd._core.errors.base import DatabandSigTermError
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.task_run.task_run_meta_files import TaskRunMetaFiles
from dbnd._core.utils import seven
from dbnd._core.utils.basics.nested_context import nested
from dbnd._core.utils.basics.signal_utils import safe_signal
from dbnd._core.utils.seven import contextlib
from dbnd._core.utils.timezone import utcnow
from dbnd_run import errors
from dbnd_run.plugin.dbnd_plugins import is_plugin_enabled, pm
from dbnd_run.run_settings import LocalEnvConfig
from dbnd_run.task_ctrl import _TaskRunExecutorCtrl
from dbnd_run.task_ctrl.task_run_orchestration_logging import (
    TaskRunOrchestrationLogManager,
)
from dbnd_run.task_ctrl.task_run_sync_local import TaskRunLocalSyncer
from dbnd_run.task_ctrl.task_sync_ctrl import TaskSyncCtrl
from dbnd_run.task_ctrl.task_validator import TaskValidator
from targets import FileTarget, target
from targets.target_config import TargetConfig


if typing.TYPE_CHECKING:
    from dbnd import Task

logger = logging.getLogger(__name__)


class TaskRunExecutor(_TaskRunExecutorCtrl):
    def __init__(self, task_run, run_executor, task_engine):
        super(TaskRunExecutor, self).__init__(task_run=task_run)

        self.deploy = TaskSyncCtrl(task_run=task_run)
        self.sync_local = TaskRunLocalSyncer(task_run=task_run)

        self.task_validator = TaskValidator(task_run=task_run)

        # custom per task engine , or just use one from global env
        self.task_engine = task_engine
        dbnd_local_root = (
            self.task_engine.dbnd_local_root or run_executor.env.dbnd_local_root
        )
        self.local_task_run_root = (
            dbnd_local_root.folder(run_executor.run_folder_prefix)
            .folder("tasks")
            .folder(self.task.task_id)
        )

        # trying to find if we should use attempt_uid that been set from external process.
        # if so - the attempt_uid is uniquely for this task_run_attempt, and that why we pop.
        task_run = self.task_run
        self.attempt_folder = self.task._meta_output.folder(
            "attempt_%s_%s"
            % (task_run.attempt_number, self.task_run.task_run_attempt_uid),
            extension=None,
        )
        self.attempt_folder_local = self.local_task_run_root.folder(
            "attempt_%s_%s" % (task_run.attempt_number, self.task_run_attempt_uid),
            extension=None,
        )
        self.attemp_folder_local_cache = self.attempt_folder_local.folder("cache")
        self.meta_files = TaskRunMetaFiles(self.attempt_folder)

        self.local_log_file: FileTarget = self.local_task_run_root.partition(
            name="%s.log" % task_run.attempt_number
        )

        self.remote_log_file = None
        if (
            not isinstance(self.task.task_env, LocalEnvConfig)
            and not self.run_executor.run_settings.run_logging.remote_logging_disabled
        ):
            self.remote_log_file: FileTarget = self.attempt_folder.partition(
                name=str(task_run.attempt_number),
                config=TargetConfig().as_file().txt,
                extension=".log",
            )

        self.log_manager = TaskRunOrchestrationLogManager(
            task_run=task_run,
            remote_log_file=self.remote_log_file,
            local_log_file=self.local_log_file,
        )

    def task_run_attempt_file(self, *path):
        return target(self.attempt_folder, *path)

    def execute(self, airflow_context=None, allow_resubmit=True, handle_sigterm=True):
        task_run = self.task_run
        run_executor = self.run_executor
        run_config = run_executor.run_config
        task = self.task  # type: Task
        task_engine = self.task_engine
        if allow_resubmit and task_engine._should_wrap_with_submit_task(task_run):
            args = task_engine.dbnd_executable + [
                "execute",
                "--dbnd-run",
                str(run_executor.driver_dump),
                "task_execute",
                "--task-id",
                task_run.task.task_id,
            ]
            submit_task = task_engine.submit_to_engine_task(
                env=task.task_env, task_name=SystemTaskName.task_submit, args=args
            )
            submit_task.descendants.add_child(task.task_id)
            if run_config.open_web_tracker_in_browser:
                webbrowser.open_new_tab(task_run.task_tracker_url)
            run_executor.run_task_at_execution_time(submit_task)
            return

        with self.task_run.task_run_track_execute(
            capture_log=False
        ) as tracking_context, self.task_run_execute_context(
            handle_sigterm=handle_sigterm
        ) as execute_context:

            if run_executor.is_killed():
                raise errors.task_execution.databand_context_killed(
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
                    self.task_validator.validate_task_inputs()

                try:
                    result = self.task._task_submit()
                    self.ctrl.save_task_band()
                    if run_config.validate_task_outputs:
                        self.task_validator.validate_task_is_complete()
                finally:
                    self.task_run.finished_time = utcnow()

                task_run.set_task_run_state(TaskRunState.SUCCESS)
                run_executor.cleanup_after_task_run(task)

                return result
            except DatabandSigTermError as ex:
                logger.error(
                    "Sig TERM! Killing the task '%s' via task.on_kill()",
                    task_run.task.task_id,
                )
                run_executor._internal_kill()

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
                run_executor._internal_kill()
                raise
            except SystemExit as ex:
                error = TaskRunError.build_from_ex(ex, task_run)
                task_run.set_task_run_state(TaskRunState.CANCELLED, error=error)
                raise errors.task_execution.system_exit_at_task_run(task, ex)
            except Exception as ex:
                error = TaskRunError.build_from_ex(ex, task_run)
                task_run.set_task_run_state(TaskRunState.FAILED, error=error)
                show_error_once.set_shown(ex)
                raise
            finally:
                task_run.airflow_context = None

    @contextlib.contextmanager
    def task_run_execute_context(self, handle_sigterm=True):
        ctx_managers = []

        if handle_sigterm:
            ctx_managers.append(handle_sigterm_at_dbnd_task_run())

        if self.run_executor.run_settings.run_logging.capture_task_run_log:
            ctx_managers.append(self.log_manager.capture_task_log())

        ctx_managers.extend(pm.hook.dbnd_task_run_context(task_run=self.task_run))

        with nested(*ctx_managers):
            yield

    def __getstate__(self):
        d = self.__dict__.copy()
        if "airflow_context" in d:
            # airflow context contains "auto generated code" that can not be pickled (Vars class)
            # we don't need to pickle it as we pickle DAGs separately
            d = self.__dict__.copy()
            del d["airflow_context"]
        return d


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
                from dbnd_run.airflow.dbnd_airflow_contrib.kubernetes_metrics_logger import (
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
