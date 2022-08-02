# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from collections import OrderedDict
from contextlib import contextmanager
from itertools import chain
from typing import Any, ContextManager, Dict, List, Optional

import six

from dbnd import dbnd_context
from dbnd._core.configuration.environ_config import ENV_DBND_SCRIPT_NAME
from dbnd._core.constants import TaskRunState, UpdateSource
from dbnd._core.tracking.airflow_dag_inplace_tracking import (
    get_task_run_uid_for_inline_script,
)
from dbnd._core.tracking.backends import TrackingStore
from dbnd._core.utils.type_check_utils import is_instance_by_class_name
from dbnd_airflow.tracking.conf_operations import flat_conf
from dbnd_airflow.tracking.config import TrackingSparkConfig
from dbnd_airflow.tracking.dbnd_airflow_conf import get_databand_url_conf
from dbnd_airflow.tracking.dbnd_spark_conf import (
    add_spark_env_fields,
    dbnd_wrap_spark_environment,
    get_databricks_python_script_name,
    spark_submit_with_dbnd_tracking,
)
from dbnd_airflow.tracking.fakes import FakeRun, FakeTask, FakeTaskRun


if typing.TYPE_CHECKING:
    from airflow.contrib.operators.databricks_operator import (
        DatabricksHook,
        DatabricksSubmitRunOperator,
    )
    from airflow.contrib.operators.ecs_operator import ECSOperator
    from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

logger = logging.getLogger(__name__)


def build_dbnd_spark_envs(tracking_info):
    # type: (Dict[str, str]) -> Dict[str, str]
    """
    Takes dbnd context as dict (airflow context + databand task context), builds spark.env properties,
    and enriches it with properties required for proper tracking in client and cluster modes
    """
    return dict(
        chain(
            add_spark_env_fields(tracking_info).items(),
            dbnd_spark_conf().items(),
            spark_deploy_mode_cluster_conf().items(),
        )
    )


def dbnd_spark_conf():
    """
    Dict with Spark properties to explicitly enable tracking
    """
    return {"spark.env.DBND__TRACKING": "True"}


def spark_deploy_mode_cluster_conf():
    """
    Dict with Spark properties to explicitly enable tracking in cluster mode
    """
    return {
        "spark.yarn.appMasterEnv.DBND__TRACKING": "True",
        "spark.yarn.appMasterEnv.SPARK_ENV_LOADED": "1",
        "spark.yarn.appMasterEnv.DBND__ENABLE__SPARK_CONTEXT_ENV": "True",
        "spark.yarn.appMasterEnv.DBND_HOME": "/tmp/dbnd",
    }


@contextmanager
def track_emr_add_steps_operator(operator, tracking_info):
    logger.info("Tracking EmrAddStepsOperator")
    spark_envs = build_dbnd_spark_envs(tracking_info)
    # EMR uses list arguments, it's safe to update it
    spark_envs.update(TrackingSparkConfig.from_databand_context().spark_conf())
    flat_spark_envs = flat_conf(spark_envs)
    for step in operator.steps:
        logger.info("Updating properties for %s", operator)
        args = step["HadoopJarStep"]["Args"]
        if args and "spark-submit" in args[0]:
            # Add dbnd and airflow context
            step["HadoopJarStep"]["Args"] = spark_submit_with_dbnd_tracking(
                args, dbnd_context=flat_spark_envs
            )
            logger.info(
                "Properties for HadoopJarStep was updated for task %s", operator.task_id
            )
        else:
            logger.info("spark-submit has been not found in the operator")
    yield


@contextmanager
def track_databricks_submit_run_operator(operator, tracking_info):
    # type: (DatabricksSubmitRunOperator, Dict[str, str])-> None
    config = operator.json
    script_name = None  # type: str
    # passing env variables is only supported in new clusters
    if "new_cluster" in config:
        cluster = config["new_cluster"]
        cluster.setdefault("spark_env_vars", {})
        cluster["spark_env_vars"].update(tracking_info)
        cluster["spark_env_vars"].update(get_databand_url_conf())
        cluster.setdefault("spark_conf", {})
        cluster["spark_conf"].update(build_dbnd_spark_envs(tracking_info))
        cluster["spark_conf"].update(
            TrackingSparkConfig.from_databand_context().merged_spark_conf(
                cluster["spark_conf"]
            )
        )

        if "spark_python_task" in config:
            cluster["spark_env_vars"].update(
                get_databricks_python_script_name(
                    config["spark_python_task"]["python_file"]
                )
            )
            script_name = cluster["spark_env_vars"][ENV_DBND_SCRIPT_NAME]
            # calculate deterministic task uids so we can use it for manual completion
            (
                task_id,
                task_run_uid,
                task_run_attempt_uid,
            ) = get_task_run_uid_for_inline_script(tracking_info, script_name)

    if "spark_jar_task" in config:
        cluster.setdefault("spark_conf", {})
        cluster["spark_conf"].update(
            TrackingSparkConfig.from_databand_context().merged_spark_conf(
                cluster["spark_conf"]
            )
        )
    yield
    if script_name:
        # When dbnd is running inside Databricks, the script can be SIGTERM'd before dbnd will send state to tracker
        # So we need to check whenever the run was succeeded afterwards and set run state manually
        tracking_store = dbnd_context().tracking_store  # type: TrackingStore
        run = FakeRun(source=UpdateSource.airflow_tracking)
        task_run = FakeTaskRun(
            task_run_uid=task_run_uid,
            task_run_attempt_uid=task_run_attempt_uid,
            run=run,
            task=FakeTask(task_name=task_id, task_id=task_id),
            task_af_id=task_id,
        )
        try:
            hook = None
            if hasattr(operator, "_get_hook"):
                hook = operator._get_hook()  # type: DatabricksHook
            elif hasattr(operator, "get_hook"):
                hook = operator.get_hook()  # type: DatabricksHook
            if hook:
                run_page_url = hook.get_run_page_url(operator.run_id)
                state = hook.get_run_state(operator.run_id)

            tracking_store = dbnd_context().tracking_store  # type: TrackingStore
            save_external_links_safe(
                tracking_store, task_run, {"databricks": run_page_url}
            )

            if state.is_successful:
                set_task_run_state_safe(tracking_store, task_run, TaskRunState.SUCCESS)
            else:
                # TODO: error should be extracted from plain Databricks logs
                set_task_run_state_safe(tracking_store, task_run, TaskRunState.FAILED)
        except Exception as exc:
            logger.error(
                "Unable to get inline script run state from Databricks. Setting task run state to Failed: %s",
                exc,
            )
            set_task_run_state_safe(tracking_store, task_run, TaskRunState.FAILED)
            return


def set_task_run_state_safe(tracking_store, task_run, state):
    # type: (TrackingStore, Any, TaskRunState) -> None
    try:
        tracking_store.set_task_run_state(task_run=task_run, state=state)
    except Exception as exc:
        logger.error("Unable to set task run state: %s", exc)


def save_external_links_safe(tracking_store, task_run, links_dict):
    # type: (TrackingStore, Any, Dict[str, str]) -> None
    try:
        tracking_store.save_external_links(
            task_run=task_run, external_links_dict=links_dict
        )
    except Exception as exc:
        logger.error("Unable to set external links: %s", exc)


@contextmanager
def track_dataproc_pyspark_operator(operator, tracking_info):
    if operator.dataproc_properties is None:
        operator.dataproc_properties = dict()
    spark_envs = build_dbnd_spark_envs(tracking_info)
    spark_envs.update(
        TrackingSparkConfig.from_databand_context().merged_spark_conf(
            operator.dataproc_properties
        )
    )
    # It's possible to pass JARs to Dataproc operator in two ways:
    # 1. `spark.jars` property
    # 2. `dataproc_jars` property
    # Those properties are mutually exclude each other
    # So if dataproc_jars are populated we're gonna add our jar here
    if operator.dataproc_jars and len(operator.dataproc_jars) > 0:
        operator.dataproc_jars = merge_dataproc_jar_uris(
            operator.dataproc_jars, spark_envs.get("spark.jars")
        )
        # del spark.jars from spark props because jars are in `jar_file_uris` property already
        spark_envs.pop("spark.jars", None)
    operator.dataproc_properties.update(spark_envs)
    yield


@contextmanager
def track_dataproc_submit_job_operator(operator, tracking_info):
    logger.info("Tracking DataprocSubmitJobOperator")
    if operator.job is not None:
        if "pyspark_job" in operator.job:
            logger.info("Updating properties for %s", operator)
            spark_envs = build_dbnd_spark_envs(tracking_info)
            spark_envs.update(TrackingSparkConfig.from_databand_context().spark_conf())

            pyspark_job = operator.job["pyspark_job"]
            pyspark_job.setdefault("properties", {})
            spark_envs.update(
                TrackingSparkConfig.from_databand_context().merged_spark_conf(
                    pyspark_job["properties"]
                )
            )
            if "jar_file_uris" in pyspark_job and len(pyspark_job["jar_file_uris"]) > 0:
                # merge `jar_file_uris` property
                pyspark_job["jar_file_uris"] = merge_dataproc_jar_uris(
                    pyspark_job["jar_file_uris"], spark_envs.get("spark.jars")
                )
                # del spark.jars from spark props because jars are in `jar_file_uris` property already
                spark_envs.pop("spark.jars", None)
            pyspark_job["properties"].update(spark_envs)
            logger.info(
                "Properties for pyspark_job was updated for task %s", operator.task_id
            )
        else:
            logger.info("pyspark_job has been not found in the operator")
    yield


def merge_dataproc_jar_uris(jar_uris, spark_jars):
    # type: (List[str], str) -> List[str]
    """
    Merge jar list, but for jar_file_uris property. spark_jars is comma-separated list of jars
    """
    if spark_jars:
        return jar_uris + spark_jars.split(",")
    else:
        return jar_uris


@contextmanager
def track_spark_submit_operator(operator, tracking_info):
    # type: (SparkSubmitOperator, Dict[str,str])-> None
    if operator._conf is None:
        operator._conf = dict()
    spark_envs = build_dbnd_spark_envs(tracking_info)
    operator._conf.update(spark_envs)
    operator._conf.update(
        TrackingSparkConfig.from_databand_context().merged_spark_conf(operator._conf)
    )

    if operator._env_vars is None:
        operator._env_vars = dict()
    dbnd_env_vars = dbnd_wrap_spark_environment()
    operator._env_vars.update(dbnd_env_vars)
    yield


@contextmanager
def track_ecs_operator(operator, tracking_info):
    # type: (ECSOperator, Dict[str,str])-> None
    """
    Adding the tracking info to the ECS environment through the `override` -> `containerOverrides`.
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
    yield


# registering operators names to the relevant tracking method
_EXECUTE_TRACKING = OrderedDict(
    [
        ("EmrAddStepsOperator", track_emr_add_steps_operator),
        ("EmrPySparkOperator", track_emr_add_steps_operator),
        ("DatabricksSubmitRunOperator", track_databricks_submit_run_operator),
        # Airflow 1
        ("DataProcPySparkOperator", track_dataproc_pyspark_operator),
        # Airflow 2
        ("DataprocSubmitJobOperator", track_dataproc_submit_job_operator),
        ("DataprocSubmitPySparkJobOperator", track_dataproc_pyspark_operator),
        ("SparkSubmitOperator", track_spark_submit_operator),
        ("ECSOperator", track_ecs_operator),
    ]
)


def wrap_operator_with_tracking_info(tracking_info, operator):
    # type: (Dict[str, str], Any) -> Optional[ContextManager]
    """
    Wrap the operator with relevant tracking method, if found such method.
    """
    for class_name, tracking_wrapper in _EXECUTE_TRACKING.items():
        logger.debug(
            " %s %s %s",
            operator,
            class_name,
            is_instance_by_class_name(operator, class_name),
        )
        if is_instance_by_class_name(operator, class_name):
            return tracking_wrapper(operator, tracking_info)
