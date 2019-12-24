import logging
import typing

from contextlib import contextmanager

import six

from dbnd._core.log.config import captures_log_into_file_as_task_file
from dbnd._core.log.logging_utils import (
    TaskContextFilter,
    redirect_stderr,
    redirect_stdout,
)
from dbnd._core.settings import LocalEnvConfig
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.utils.string_utils import safe_short_string


if typing.TYPE_CHECKING:
    pass
logger = logging.getLogger(__name__)

CURRENT_TASK_HANDLER_LOG = None


class TaskRunLogManager(TaskRunCtrl):
    def __init__(self, task_run):
        super(TaskRunLogManager, self).__init__(task_run)

        self.local_log_file = self.task_run.local_task_run_root.partition(
            name="task.log"
        )
        self.remote_log_file = None
        if not isinstance(self.task.task_env, LocalEnvConfig):
            self.remote_log_file = self.task_run.attempt_folder.partition("task.log")

        # file handler for task log
        # if set -> we are in the context of capturing
        self.local_log_handler_enabled = False

    @contextmanager
    def capture_task_log(self):
        global CURRENT_TASK_HANDLER_LOG
        if self.local_log_handler_enabled or not self.task.settings.log.task_file_log:
            yield None
            return

        error_happened = False
        try:
            with self.capture_stderr_stdout(), TaskContextFilter.task_context(
                self.task.task_id
            ), captures_log_into_file_as_task_file(self.local_log_file.path) as handler:
                self.local_log_handler_enabled = True
                CURRENT_TASK_HANDLER_LOG = handler
                yield handler
        except Exception as task_ex:
            CURRENT_TASK_HANDLER_LOG = None
            error_happened = True
            raise task_ex
        finally:
            self.local_log_handler_enabled = False
            try:
                log_body = self.read_log_body()
                self.write_remote_log(log_body)
                self.save_log_preview(log_body)
            except Exception as save_log_ex:
                if error_happened:
                    logger.error(
                        "failed to save log after task failure: %s", save_log_ex
                    )
                else:
                    raise save_log_ex

    def read_log_body(self):
        try:
            log_body = self.local_log_file.read()
            if six.PY2:
                log_body = log_body.decode("utf-8")
            return log_body
        except Exception as ex:
            logger.error(
                "Failed to read log (%s) for %s: %s",
                self.local_log_file.path,
                self.task,
                ex
            )
            return None

    def write_remote_log(self, log_body):
        if self.task.settings.log.remote_logging_disabled or not self.remote_log_file:
            return

        try:
            self.remote_log_file.write(log_body)
        except Exception as ex:
            # todo add remote log path to error
            logger.warning("Failed to write remote log for %s: %s", self.task, ex)

    def save_log_preview(self, log_body):
        max_size = self.task.settings.log.send_body_to_server_max_size
        if max_size == 0:  # use 0 for unlimited
            log_preview = log_body
        elif max_size == -1: # use -1 to disable
            log_preview = None
        else:
            log_preview = self._extract_log_preivew(log_body=log_body, max_size=max_size)
        if log_preview:
            self.task_run.tracker.save_task_run_log(log_preview)

    def _extract_log_preivew(self, log_body=None, max_size=1000):
        is_tail_preview = max_size > 0  # pass negative to get log's 'head' instead of 'tail'
        return safe_short_string(log_body, abs(max_size), tail=is_tail_preview)

    @contextmanager
    def capture_stderr_stdout(self, logging_target=None):
        logging_target_stdout = logging_target or logging.getLogger("dbnd.stdout")
        logging_target_stderr = logging_target or logging.getLogger("dbnd.stderr")
        with redirect_stdout(logging_target_stderr, logging.INFO), redirect_stderr(
            logging_target_stdout, logging.WARN
        ):
            yield
