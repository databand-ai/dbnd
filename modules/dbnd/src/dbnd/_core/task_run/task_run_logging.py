# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from contextlib import contextmanager

from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.log.buffered_memory_handler import BufferedMemoryHandler
from dbnd._core.log.logging_utils import capture_stderr_stdout
from dbnd._core.settings import TrackingLoggingConfig
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl


if typing.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class TaskRunLogManager(TaskRunCtrl):
    def __init__(self, task_run):
        super(TaskRunLogManager, self).__init__(task_run)

        # file handler for task log
        # if set -> we are in the context of capturing
        self._log_task_run_into_file_active = False

    @contextmanager
    def capture_task_log(self):
        if self._log_task_run_into_file_active:
            yield None
            return
        log_settings: TrackingLoggingConfig = self.settings.tracking_log
        if not log_settings.capture_tracking_log or not (
            log_settings.preview_head_bytes or log_settings.preview_head_bytes
        ):  # nothing to upload:
            yield None
            return

        if self.task.task_family.endswith("_execute"):
            yield None
            return

        handler = BufferedMemoryHandler(
            max_head_bytes=log_settings.preview_head_bytes,
            max_tail_bytes=log_settings.preview_tail_bytes,
        )
        handler.setFormatter(logging.Formatter(fmt=log_settings.formatter))
        handler.setLevel(logging.INFO)

        target_logger = logging.root

        if not handler or not target_logger:
            yield None
            return

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
            except Exception as ex:
                log_exception(
                    "Failed to close file handler for log", ex=ex, non_critical=True
                )

            self._log_task_run_into_file_active = False
            self._upload_task_log_preview(handler)

    def _upload_task_log_preview(self, log_handler):
        try:
            if isinstance(log_handler, BufferedMemoryHandler):
                log_body = log_handler.get_log_body()
            else:
                logger.exception("unable to read log body for %s", self)
                return

        except Exception as read_log_err:
            logger.exception("failed to read log preview for %s:%s", self, read_log_err)
            return

        try:
            self.task_run.tracker.save_task_run_log(log_body)
        except Exception as save_log_err:
            logger.exception(
                "failed to save task run log preview for %s:%s", self, save_log_err
            )
