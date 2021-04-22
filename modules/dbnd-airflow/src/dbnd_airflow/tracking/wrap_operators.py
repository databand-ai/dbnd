import typing

from collections import OrderedDict
from typing import Dict

import six

from dbnd._core.utils.type_check_utils import is_instance_by_class_name
from dbnd_airflow.tracking.conf_operations import flat_conf
from dbnd_airflow.tracking.dbnd_airflow_conf import get_databand_url_conf
from dbnd_airflow.tracking.dbnd_spark_conf import (
    add_spark_env_fields,
    dbnd_wrap_spark_environment,
    get_databricks_java_agent_conf,
    get_spark_submit_java_agent_conf,
    spark_submit_with_dbnd_tracking,
)


if typing.TYPE_CHECKING:
    from airflow.models import BaseOperator
    from airflow.contrib.operators.ecs_operator import ECSOperator
    from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


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
    # type: (SparkSubmitOperator, Dict[str,str])-> None

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


def track_ecs_operator(operator, tracking_info):
    # type: (ECSOperator, Dict[str,str])-> None
    """
    Adding the the tracking info to the ECS environment through the `override` -> `containerOverrides`.
    Notice that we require to have `overrides` and `containerOverrides` with containers names in-ordered to make it work

    Airflow pass the override to boto so here is the boto3 docs:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecs.html#ECS.Client.run_task
    """
    info_as_env_var = [
        {"name": key, "value": value} for key, value in six.iteritems(tracking_info)
    ]

    new = []
    if "containerOverrides" in operator.overrides:
        for override in operator.overrides["containerOverrides"]:
            override.setdefault("environment", [])
            override["environment"].extend(info_as_env_var)
            new.append(override)

        operator.overrides["containerOverrides"] = new


# registering operators names to the relevant tracking method
_EXECUTE_TRACKING = OrderedDict(
    [
        ("EmrAddStepsOperator", track_emr_add_steps_operator),
        ("DatabricksSubmitRunOperator", track_databricks_submit_run_operator),
        ("DataProcPySparkOperator", track_data_proc_pyspark_operator),
        ("SparkSubmitOperator", track_spark_submit_operator),
        ("ECSOperator", track_ecs_operator),
    ]
)


def wrap_operator_with_tracking_info(tracking_info, operator):
    # type: (Dict[str, str], BaseOperator) -> None
    """
    Wrap the operator with relevant tracking method, if found such method.
    """
    for class_name, tracking_wrapper in _EXECUTE_TRACKING.items():
        if is_instance_by_class_name(operator, class_name):
            tracking_wrapper(operator, tracking_info)
            break
