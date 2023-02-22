# © Copyright Databand.ai, an IBM Company 2022
import logging

from dbnd._core.utils.basics.environ_utils import environ_enabled
from dbnd.providers.spark.dbnd_spark_init import _safe_get_jvm_view


logger = logging.getLogger(__name__)


# Logging
def attach_spark_logger(spark_log_file):
    if environ_enabled("DBND__LOG_SPARK"):
        try:
            log4j, spark_logger = try_get_spark_logger()
            if log4j is None:
                return

            pattern = "[%d] {%c,%C{1}} %p - %m%n"
            file_appender = log4j.FileAppender()

            file_appender.setFile(spark_log_file.path)
            file_appender.setName(spark_log_file.path)
            file_appender.setLayout(log4j.PatternLayout(pattern))
            file_appender.setThreshold(log4j.Priority.toPriority("INFO"))
            file_appender.activateOptions()
            spark_logger.addAppender(file_appender)
        except Exception as task_ex:
            logger.warning(
                "Failed to attach spark logger for log %s: %s", spark_log_file, task_ex
            )


def detach_spark_logger(spark_log_file):
    if environ_enabled("DBND__LOG_SPARK"):
        try:
            log4j, spark_logger = try_get_spark_logger()
            if log4j is None:
                return

            spark_logger.removeAppender(spark_log_file.path)
        except Exception as task_ex:
            logger.warning(
                "Failed to detach spark logger for log %s: %s", spark_log_file, task_ex
            )


def try_get_spark_logger():
    try:
        jvm = _safe_get_jvm_view()
        if jvm is None:
            logger.warning(
                "Spark log is enabled but SparkContext is not available. Consider switching DBND__LOG_SPARK to False."
            )
            return None, None
        log4j = jvm.org.apache.log4j
        return log4j, log4j.Logger.getLogger("org.apache.spark")
    except Exception:
        logger.warning(
            "Failed to retrieve log4j context from Spark Context. Spark logs won't be captured."
        )
        return None, None
