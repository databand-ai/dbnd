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


def verify_spark_pre_conditions():
    if spark_tracking_enabled() and _SPARK_ENV_FLAG in os.environ:
        if _is_dbnd_spark_installed():
            return True
        else:
            _debug_init_print("failed to import pyspark or dbnd-spark")
    else:
        _debug_init_print(
            "DBND__ENABLE__SPARK_CONTEXT_ENV or SPARK_ENV_LOADED are not set"
        )
    return False


def _safe_get_active_spark_context():
    if not verify_spark_pre_conditions():
        return None
    try:
        from pyspark import SparkContext

        if SparkContext._jvm is not None:
            return SparkContext._active_spark_context
        else:
            # spark context is not initialized at this step
            logger.info("SparkContext._jvm is not set")
    except Exception as ex:
        logger.info("Failed to get SparkContext: %s", ex)


def _safe_get_jvm_view():
    try:
        spark_context = _safe_get_active_spark_context()
        if spark_context is not None:
            return spark_context._jvm
    except Exception as ex:
        logger.info("Failed to get jvm from SparkContext : %s", ex)


def _safe_get_spark_conf():
    try:
        spark_context = _safe_get_active_spark_context()
        if spark_context is not None:
            return spark_context.getConf()
    except Exception as ex:
        logger.info("Failed to get SparkConf from SparkContext : %s", ex)


def try_get_airflow_context_from_spark_conf():
    # type: ()-> Optional[AirflowTaskContext]
    try:
        conf = _safe_get_spark_conf()
        if conf is not None:
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
            else:
                logger.warning("Airflow context could not be loaded from spark conf")
    except Exception as ex:
        logger.info("Failed to get airflow context info from spark job: %s", ex)

    return None


def get_value_from_spark_env(key):
    try:
        conf = _safe_get_spark_conf()
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
    try:
        jvm = _safe_get_jvm_view()
        if jvm is None:
            return
        jvm_dbnd = jvm.ai.databand.DbndWrapper

        from py4j import java_gateway

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
