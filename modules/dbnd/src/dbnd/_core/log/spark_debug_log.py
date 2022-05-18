import os
import pprint
import sys

from dbnd import dbnd_tracking_start, log_metric
from dbnd._core.tracking.dbnd_spark_init import (
    _is_dbnd_spark_installed,
    try_get_airflow_context_from_spark_conf,
    verify_spark_pre_conditions,
)


pp = pprint.PrettyPrinter(indent=4)


def custom_print(msg):
    print(msg, file=sys.stderr)


def _is_dbnd_airflow_key(k):
    return "dbnd" in k.lower() or "airflow" in k.lower()


def print_dbnd_environment_info():
    custom_print("DBND ENVIRONMENT INFO")
    custom_print("ENVIRON:\n %s" % pp.pformat(os.environ))
    custom_print(
        "DBND AIRFLOW ENVIRON:\n %s"
        % pp.pformat({k: v for k, v in os.environ.items() if _is_dbnd_airflow_key(k)})
    )

    custom_print("SYS PATH:\n %s" % pp.pformat(sys.path))
    custom_print("PYTHON:\n %s" % pp.pformat(sys.executable))


def print_dbnd_spark_info():
    custom_print("IS_DBND_SPARK_INSTALLED:  %s" % _is_dbnd_spark_installed())
    custom_print("VERIFY_SPARK_PRE_CONDITIONS: %s" % verify_spark_pre_conditions())
    print_airflow_context()

    from pyspark import SparkConf

    spark_conf = SparkConf().getAll()
    custom_print("DBND SPARK INFO")
    custom_print("SPARK GET CONF %s" % pp.pformat(spark_conf))
    custom_print(
        "DBND AIRFLOW SPARK GET CONF\n %s"
        % pp.pformat([(k, v) for k, v in spark_conf if _is_dbnd_airflow_key(k)])
    )


def print_airflow_context():
    custom_print("DBND SPARK AIRFLOW CONTEXT")
    try:
        af_context = try_get_airflow_context_from_spark_conf()
        if af_context:
            custom_print("DAG_ID: %s" % af_context.dag_id)
            custom_print("EXECUTION_DATE: %s" % af_context.execution_date)
            custom_print("TASK_ID: %s" % af_context.task_id)
            custom_print("TRY_NUMBER: %s" % af_context.try_number)
            custom_print("AIRFLOW_INSTANCE_UID: %s" % af_context.airflow_instance_uid)
        else:
            custom_print("DBND AIRFLOW CONTEXT is None")
    except Exception as e:
        custom_print("airflow context exception: %s" % pp.pformat(e))


def print_dbnd_debug():
    try:
        print_dbnd_environment_info()
        print_dbnd_spark_info()
    except Exception as e:
        custom_print("debug spark init exception: %s" % pp.pformat(e))


def dbnd_debug_spark_integration():
    # print debug vars before calling dbnd_tracking_start
    print_dbnd_debug()

    # Try to "manually" start dbnd tracking
    try:
        task_run = dbnd_tracking_start()
        custom_print("DBND_TRACKING_TASK_RUN: %s" % task_run)
        if task_run:
            custom_print("DBND_TRACKING_RUN_UID: %s" % task_run.run.run_uid)
    except Exception as e:
        custom_print("dbnd tracking task run exception: %s" % e)

    # test log metric
    log_metric("debug.metric", 10000)

    # print debug vars after calling dbnd_tracking_start
    print_dbnd_debug()
