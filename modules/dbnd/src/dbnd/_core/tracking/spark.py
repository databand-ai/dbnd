import logging
import os

from typing import Optional

from dbnd._core.configuration.environ_config import (
    _debug_init_print,
    spark_tracking_enabled,
)
from dbnd._core.tracking.airflow_task_context import AirflowTaskContext
from dbnd._core.utils.basics.environ_utils import environ_enabled
from dbnd._core.utils.seven import import_errors


_IS_SPARK_INSTALLED = None
_SPARK_ENV_FLAG = "SPARK_ENV_LOADED"  # if set, we are in spark

logger = logging.getLogger(__name__)


def _is_dbnd_spark_installed():
    global _IS_SPARK_INSTALLED
    if _IS_SPARK_INSTALLED is not None:
        return _IS_SPARK_INSTALLED
    try:
        try:
            from pyspark import SparkContext  # noqa: F401

            from dbnd_spark import dbnd_spark_bootstrap

            dbnd_spark_bootstrap()
        except import_errors:
            _IS_SPARK_INSTALLED = False
        else:
            _IS_SPARK_INSTALLED = True
    except Exception:
        # safeguard, on any exception
        _IS_SPARK_INSTALLED = False

    return _IS_SPARK_INSTALLED


def try_get_airflow_context_from_spark_conf():
    # type: ()-> Optional[AirflowTaskContext]
    if not spark_tracking_enabled() or _SPARK_ENV_FLAG not in os.environ:
        _debug_init_print(
            "DBND__ENABLE__SPARK_CONTEXT_ENV or SPARK_ENV_LOADED are not set"
        )
        return None

    if not _is_dbnd_spark_installed():
        _debug_init_print("failed to import pyspark or dbnd-spark")
        return None
    try:
        _debug_init_print("creating spark context to get spark conf")
        from pyspark import SparkContext

        # we shouldn't instantiate SparkContext
        conf = SparkContext.getOrCreate().getConf()

        dag_id = conf.get("spark.env.AIRFLOW_CTX_DAG_ID")
        execution_date = conf.get("spark.env.AIRFLOW_CTX_EXECUTION_DATE")
        task_id = conf.get("spark.env.AIRFLOW_CTX_TASK_ID")
        try_number = conf.get("spark.env.AIRFLOW_CTX_TRY_NUMBER")
        airflow_instance_uid = conf.get("spark.env.AIRFLOW_CTX_UID")

        if dag_id and task_id and execution_date:
            return AirflowTaskContext(
                dag_id=dag_id,
                execution_date=execution_date,
                task_id=task_id,
                try_number=try_number,
                airflow_instance_uid=airflow_instance_uid,
            )
    except Exception as ex:
        logger.info("Failed to get airflow context info from spark job: %s", ex)

    return None


def get_value_from_spark_env(key):
    # spark guards
    if not spark_tracking_enabled() or _SPARK_ENV_FLAG not in os.environ:
        return None

    if not _is_dbnd_spark_installed():
        return None

    try:
        from pyspark import SparkContext

        conf = SparkContext.getOrCreate().getConf()
        value = conf.get("spark.env." + key)
        if value:
            return value
    except:
        return None


def set_current_jvm_context(run_uid, task_run_uid, task_run_attempt_uid, task_af_id):
    """
    When pyspark is called on the first place we want to ensure that spark listener will report metrics
    to the proper task. To achieve this, we directly set current context to our JVM wrapper.
    :return:
    """
    if not spark_tracking_enabled() or _SPARK_ENV_FLAG not in os.environ:
        return
    try:
        from py4j import java_gateway
        from pyspark import SparkContext

        if SparkContext._jvm is None:
            # spark context is not initialized at this step
            return

        jvm_dbnd = SparkContext._jvm.ai.databand.DbndWrapper
        if isinstance(jvm_dbnd, java_gateway.JavaPackage):
            # if DbndWrapper class is not loaded then agent or IO listener is not attached
            return
        try:
            jvm_dbnd.instance().setExternalTaskContext(
                str(run_uid),
                str(task_run_uid),
                str(task_run_attempt_uid),
                str(task_af_id),
            )
        except Exception as jvm_ex:
            logger.info(
                "Failed to set DBND context to JVM during DbndWrapper call: %s", jvm_ex
            )
    except Exception as ex:
        logger.info("Failed to set DBND context to JVM: %s", ex)


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
        from pyspark import SparkContext
    except Exception:
        logger.warning(
            "Spark log is enabled but pyspark is not available. Consider switching DBND__LOG_SPARK to False."
        )
        # pyspark is not available, just pass
        return None, None

    try:
        if SparkContext._jvm is None:
            # spark context is not initialized at this step and JVM is not available
            return None, None

        jvm = SparkContext._jvm
        log4j = jvm.org.apache.log4j
        return log4j, log4j.Logger.getLogger("org.apache.spark")
    except Exception:
        logger.warning(
            "Failed to retrieve log4j context from Spark Context. Spark logs won't be captured."
        )
        return None, None
