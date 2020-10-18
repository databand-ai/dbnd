import logging

from collections import OrderedDict
from typing import Optional

import six

from dbnd._core.constants import TaskRunState
from dbnd._core.decorator.dbnd_func_proxy import DbndFuncProxy, _log_result, task
from dbnd._core.inplace_run.airflow_dag_inplace_tracking import extract_airflow_context
from dbnd._core.inplace_run.inplace_run_manager import dbnd_run_start, dbnd_run_stop
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.utils.type_check_utils import is_instance_by_class_name
from dbnd_airflow.tracking.conf_operations import flat_conf
from dbnd_airflow.tracking.dbnd_airflow_conf import (
    get_databand_url_conf,
    get_tracking_information,
)
from dbnd_airflow.tracking.dbnd_spark_conf import (
    add_spark_env_fields,
    dbnd_wrap_spark_environment,
    get_databricks_java_agent_conf,
    get_spark_submit_java_agent_conf,
    spark_submit_with_dbnd_tracking,
)


logger = logging.getLogger(__name__)


def track_emr_add_steps_operator(operator, tracking_info):
    flat_spark_envs = flat_conf(add_spark_env_fields(tracking_info))
    for step in operator.steps:
        args = step["HadoopJarStep"]["Args"]
        if args and "spark-submit" in args[0]:
            step["HadoopJarStep"]["Args"] = spark_submit_with_dbnd_tracking(
                args, dbnd_context=flat_spark_envs
            )


def track_databricks_submit_run_operator(operator, tracking_info):
    config = operator.json
    # passing env variables is only supported in new clusters
    if "new_cluster" in config:
        cluster = config["new_cluster"]
        cluster.setdefault("spark_env_vars", {})
        cluster["spark_env_vars"].update(tracking_info)
        cluster["spark_env_vars"].update(get_databand_url_conf())

        if "spark_jar_task" in config:
            cluster.setdefault("spark_conf", {})
            agent_conf = get_databricks_java_agent_conf()
            if agent_conf is not None:
                cluster["spark_conf"].update(agent_conf)


def track_data_proc_pyspark_operator(operator, tracking_info):
    if operator.dataproc_properties is None:
        operator.dataproc_properties = dict()
    spark_envs = add_spark_env_fields(tracking_info)
    operator.dataproc_properties.update(spark_envs)


def track_spark_submit_operator(operator, tracking_info):
    if operator._conf is None:
        operator._conf = dict()
    spark_envs = add_spark_env_fields(tracking_info)
    operator._conf.update(spark_envs)

    if operator._env_vars is None:
        operator._env_vars = dict()
    dbnd_env_vars = dbnd_wrap_spark_environment()
    operator._env_vars.update(dbnd_env_vars)

    if _has_java_application(operator):
        agent_conf = get_spark_submit_java_agent_conf()
        if agent_conf is not None:
            operator._conf.update(agent_conf)


def _has_java_application(operator):
    return (
        operator._application.endswith(".jar")
        or operator._jars
        and operator._jars.ends_with(".jar")
    )


def track_with_env_variables(operator, tracking_info):
    import os

    os.environ.update(tracking_info)


_EXECUTE_TRACKING = OrderedDict(
    [
        ("EmrAddStepsOperator", track_emr_add_steps_operator),
        ("DatabricksSubmitRunOperator", track_databricks_submit_run_operator),
        ("DataProcPySparkOperator", track_data_proc_pyspark_operator),
        ("SparkSubmitOperator", track_spark_submit_operator),
        # we can't be sure that the auto-tracking will patch `context_to_airflow_vars`
        ("PythonOperator", track_with_env_variables),
        ("BashOperator", track_with_env_variables),
    ]
)


def new_execute(context):
    """
    This function replaces the operator's original `execute` function
    """
    # IMPORTANT!!: copied_operator:
    # ---------------------------------------
    # The task (=operator) is copied when airflow enters to TaskInstance._run_raw_task.
    # Then, only the copy_task (=copy_operator) is changed or called (render jinja, signal_handler,
    # pre_execute, execute, etc..).
    copied_operator = context["task_instance"].task
    try:
        # start operator execute run with current airflow context
        task_context = extract_airflow_context(context)
        task_run = dbnd_run_start(
            airflow_context=task_context
        )  # type: Optional[TaskRun]

        # custom manipulation for each operator
        if task_run:
            tracking_info = get_tracking_information(context, task_run)
            add_tracking_to_submit_task(tracking_info, copied_operator)

    except Exception as e:
        task_run = None
        logger.error(
            "exception caught will running on dbnd new execute {}".format(e),
            exc_info=True,
        )

    # running the operator's original execute function
    try:
        if hasattr(copied_operator, "_tracked_instance"):
            result = copied_operator.__execute__(copied_operator, context)
        elif hasattr(copied_operator, "_tracked_class"):
            result = copied_operator.__class__.__execute__(copied_operator, context)
        else:
            raise AttributeError("tracked failed to run original operator.execute")

    except Exception:
        if task_run:
            task_run.set_task_run_state(state=TaskRunState.FAILED)
        dbnd_run_stop()
        raise

    else:
        if task_run:
            _log_result(task_run, result)

    dbnd_run_stop()
    return result


def add_tracking_to_submit_task(tracking_info, operator):
    for class_name, tracking_wrapper in _EXECUTE_TRACKING.items():
        if is_instance_by_class_name(operator, class_name):
            tracking_wrapper(operator, tracking_info)
            break
