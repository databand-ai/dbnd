# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from contextlib import contextmanager

from dbnd._core.log.log_preview import read_dbnd_log_preview
from dbnd._core.log.logging_utils import capture_stderr_stdout
from dbnd_run.run_settings import RunLoggingConfig
from dbnd_run.task_ctrl import _TaskRunExecutorCtrl


if typing.TYPE_CHECKING:
    from targets import FileTarget

logger = logging.getLogger(__name__)


class TaskRunOrchestrationLogManager(_TaskRunExecutorCtrl):
    def __init__(self, task_run, local_log_file, remote_log_file, extra_log_file=None):
        super(TaskRunOrchestrationLogManager, self).__init__(task_run)

        self.local_log_file: FileTarget = local_log_file

        self.extra_log_file = extra_log_file

        self.remote_log_file: FileTarget = remote_log_file

        # file handler for task log
        # if set -> we are in the context of capturing
        self._log_task_run_into_file_active = False

    @contextmanager
    def capture_task_log(self):
        log_file = self.local_log_file

        run_logging: RunLoggingConfig = self.run_executor.run_settings.run_logging
        if self._log_task_run_into_file_active:
            yield None
            return

        if (
            # there is no really any good way to find if this is runtime tracking for airflow operator
            # should be refactored as soon as possible
            self.task.task_family.endswith("_execute")
        ):
            yield None
            return

        handler = run_logging.get_task_log_file_handler(log_file)
        logger.debug("Capturing task log into '%s'", log_file)

        if not handler:
            yield None
            return
        target_logger = logging.root

        try:
            target_logger.addHandler(handler)
            self._log_task_run_into_file_active = True

            if self.task.settings.tracking_log.capture_stdout_stderr:
                with capture_stderr_stdout():
                    yield handler
            else:
                yield handler

        except Exception as task_ex:
            raise task_ex
        finally:
            try:
                target_logger.removeHandler(handler)
                handler.close()
            except Exception:
                logger.warning("Failed to close file handler for log %s", log_file)

            self._log_task_run_into_file_active = False
            self._upload_task_log_preview_to_databand(handler)

    def _upload_task_log_preview_to_databand(self, log_handler):
        try:
            if isinstance(log_handler, logging.FileHandler):
                log_body = self.read_log_body()
            else:
                logger.exception("unable to read log body for %s", self)
                return
        except Exception as read_log_err:
            logger.exception("failed to read log preview for %s:%s", self, read_log_err)
        else:
            try:
                self.task_run.tracker.save_task_run_log(log_body, self.local_log_file)
            except Exception as save_log_err:
                logger.exception(
                    "failed to save task run log preview for %s:%s", self, save_log_err
                )

        self.write_remote_log()

    def read_log_body(self):
        try:
            return read_dbnd_log_preview(self.local_log_file.path, self.extra_log_file)

        except Exception as ex:
            logger.warning(
                "Failed to read log (%s) for %s: %s",
                self.local_log_file.path,
                self.task,
                ex,
            )
            return None

    def write_remote_log(self):
        if not self.run_executor.run_settings.run_logging.remote_logging_disabled:
            return

        if self.remote_log_file is None or self.local_log_file is None:
            return

        try:
            self.remote_log_file.copy_from_local(self.local_log_file.path)

        except Exception as ex:
            logger.warning(
                "Failed to upload the log from {local_path} "
                "to the remote log at {remote_log} "
                "for task {task}: {exception}".format(
                    local_path=self.local_log_file.path,
                    remote_log=self.remote_log_file.path,
                    task=self.task,
                    exception=ex,
                )
            )
