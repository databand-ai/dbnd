import logging
import os
import typing

from contextlib import contextmanager

import six

from dbnd._core.log.logging_utils import find_handler, redirect_stderr, redirect_stdout
from dbnd._core.settings import LocalEnvConfig
from dbnd._core.settings.log import _safe_is_typeof
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.utils.string_utils import merge_dbnd_and_spark_logs, safe_short_string


if typing.TYPE_CHECKING:
    pass
logger = logging.getLogger(__name__)

CURRENT_TASK_HANDLER_LOG = None


class TaskRunLogManager(TaskRunCtrl):
    def __init__(self, task_run):
        super(TaskRunLogManager, self).__init__(task_run)

        self.local_log_file = self.task_run.local_task_run_root.partition(
            name="%s.log" % task_run.attempt_number
        )

        if os.getenv("DBND__LOG_SPARK"):
            self.local_spark_log_file = self.task_run.local_task_run_root.partition(
                name="%s-spark.log" % task_run.attempt_number
            )

        self.local_heartbeat_log_file = self.task_run.local_task_run_root.partition(
            name="%s.heartbeat.log" % task_run.attempt_number
        )
        self.remote_log_file = None
        if not isinstance(self.task.task_env, LocalEnvConfig):
            self.remote_log_file = self.task_run.attempt_folder.partition(
                "%s.log" % task_run.attempt_number
            )

        # file handler for task log
        # if set -> we are in the context of capturing
        self._log_task_run_into_file_active = False

    @contextmanager
    def capture_stderr_stdout(self, logging_target=None):
        #  redirecting all messages from sys.stderr/sys.stdout into logging_target
        # WARNING: this can create stdout/stderr loop if :
        #     we redirect into logger, and logger is writing to current sys.stdout that is current redirect
        #     ( not the original one before the wrapping)
        #    -> LOOP:   some print ->  redirect_stdout() -> logger -> print -> redirect_stdout() ...
        if not self.task.settings.log.capture_stdout_stderr:
            yield None
            return

        airflow_root_console_handler = find_handler(logging.root, "console")
        if _safe_is_typeof(airflow_root_console_handler, "RedirectStdHandler"):
            yield None
            return

        logging_target_stdout = logging_target or logging.getLogger("dbnd.stdout")
        logging_target_stderr = logging_target or logging.getLogger("dbnd.stderr")
        with redirect_stdout(logging_target_stderr, logging.INFO), redirect_stderr(
            logging_target_stdout, logging.WARN
        ):
            yield

    @contextmanager
    def capture_task_log(self):
        global CURRENT_TASK_HANDLER_LOG
        log_file = self.local_log_file

        log_settings = self.task.settings.log
        if (
            self._log_task_run_into_file_active
            or not log_settings.capture_task_run_log
            or not log_file
        ):
            yield None
            return

        handler = log_settings.get_task_log_file_handler(log_file)
        if not handler:
            yield None
            return
        target_logger = logging.root
        logger.debug("Capturing task log into '%s'", log_file)

        try:
            self.attach_spark_logger()
            target_logger.addHandler(handler)
            self._log_task_run_into_file_active = True
            CURRENT_TASK_HANDLER_LOG = handler

            with self.capture_stderr_stdout():
                yield handler
        except Exception as task_ex:
            CURRENT_TASK_HANDLER_LOG = None
            raise task_ex
        finally:
            self.detach_spark_logger()
            try:
                target_logger.removeHandler(handler)
                handler.close()
            except Exception:
                logger.error("Failed to close file handler for log %s", log_file)
            self._log_task_run_into_file_active = False
            self._upload_task_log_preview()

    def attach_spark_logger(self):
        if os.getenv("DBND__LOG_SPARK"):
            spark_log_file = self.local_spark_log_file
            try:
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
                log4j = spark._jvm.org.apache.log4j

                spark_logger = log4j.Logger.getLogger("org.apache.spark")

                pattern = "[%d] {%c,%C{1}} %p - %m%n"
                file_appender = log4j.FileAppender()

                file_appender.setFile(spark_log_file.path)
                file_appender.setName(spark_log_file.path)
                file_appender.setLayout(log4j.PatternLayout(pattern))
                file_appender.setThreshold(log4j.Priority.toPriority("INFO"))
                file_appender.activateOptions()
                spark_logger.addAppender(file_appender)
            except Exception as task_ex:
                logger.error(
                    "Failed to attach spark logger for log %s: %s",
                    spark_log_file,
                    task_ex,
                )

    def detach_spark_logger(self):
        if os.getenv("DBND__LOG_SPARK"):
            spark_log_file = self.local_spark_log_file
            try:
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()

                jvm = spark._jvm
                log4j = jvm.org.apache.log4j
                spark_logger = log4j.Logger.getLogger("org.apache.spark")

                spark_logger.removeAppender(spark_log_file.path)
            except Exception as task_ex:
                logger.error(
                    "Failed to detach spark logger for log %s: %s",
                    spark_log_file,
                    task_ex,
                )

    def _upload_task_log_preview(self):
        try:
            log_body = self.read_log_body()
            self.write_remote_log(log_body)
            self.save_log_preview(log_body)
        except Exception as save_log_ex:
            logger.error("failed to save log preview for %s:%s", self, save_log_ex)

    def read_log_body(self):
        try:
            log_body = self.local_log_file.readlines()
            if os.getenv("DBND__LOG_SPARK"):
                spark_log_body = self.local_spark_log_file.readlines()
                merged_logs = merge_dbnd_and_spark_logs(log_body, spark_log_body)
                log_body = "\n".join(merged_logs)
            else:
                log_body = "\n".join(log_body)
            if six.PY2:
                log_body = log_body.decode("utf-8")
            return log_body
        except Exception as ex:
            logger.error(
                "Failed to read log (%s) for %s: %s",
                self.local_log_file.path,
                self.task,
                ex,
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
        elif max_size == -1:  # use -1 to disable
            log_preview = None
        else:
            log_preview = self._extract_log_preivew(
                log_body=log_body, max_size=max_size
            )
        if log_preview:
            self.task_run.tracker.save_task_run_log(log_preview)

    def _extract_log_preivew(self, log_body=None, max_size=1000):
        is_tail_preview = (
            max_size > 0
        )  # pass negative to get log's 'head' instead of 'tail'
        return safe_short_string(log_body, abs(max_size), tail=is_tail_preview)
