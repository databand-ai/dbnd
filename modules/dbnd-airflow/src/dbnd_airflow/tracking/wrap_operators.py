import logging
import typing

from collections import OrderedDict
from contextlib import contextmanager
from typing import Any, ContextManager, Dict, Optional

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
from dbnd_airflow.tracking.dbnd_airflow_conf import get_databand_url_conf
from dbnd_airflow.tracking.dbnd_spark_conf import (
    add_spark_env_fields,
    dbnd_wrap_spark_environment,
    get_databricks_java_agent_conf,
    get_databricks_python_script_name,
    get_spark_submit_java_agent_conf,
    spark_submit_with_dbnd_tracking,
)
from dbnd_airflow.tracking.fakes import FakeRun, FakeTask, FakeTaskRun


if typing.TYPE_CHECKING:
    from airflow.contrib.hooks.databricks_hook import RunState as DatabricksRunState
    from airflow.contrib.operators.databricks_operator import (
        DatabricksHook,
        DatabricksSubmitRunOperator,
    )
    from airflow.contrib.operators.ecs_operator import ECSOperator
    from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

logger = logging.getLogger(__name__)


@contextmanager
def track_emr_add_steps_operator(operator, tracking_info):
    flat_spark_envs = flat_conf(add_spark_env_fields(tracking_info))
    for step in operator.steps:
        args = step["HadoopJarStep"]["Args"]
        if args and "spark-submit" in args[0]:
            step["HadoopJarStep"]["Args"] = spark_submit_with_dbnd_tracking(
                args, dbnd_context=flat_spark_envs
            )
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
        agent_conf = get_databricks_java_agent_conf()
        if agent_conf is not None:
            cluster["spark_conf"].update(agent_conf)
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
            hook = operator.get_hook()  # type: DatabricksHook
            state = hook.get_run_state(operator.run_id)  # type: DatabricksRunState
            run_page_url = hook.get_run_page_url(operator.run_id)
        except Exception as exc:
            logger.error(
                "Unable to get inline script run state from Databricks. Setting task run state to Failed: %s",
                exc,
            )
            set_task_run_state_safe(tracking_store, task_run, TaskRunState.FAILED)
            return

        save_extrnal_links_safe(tracking_store, task_run, {"databricks": run_page_url})
        if state.is_successful:
            set_task_run_state_safe(tracking_store, task_run, TaskRunState.SUCCESS)
        else:
            # TODO: error should be extracted from plain Databricks logs
            set_task_run_state_safe(tracking_store, task_run, TaskRunState.FAILED)


def set_task_run_state_safe(tracking_store, task_run, state):
    # (TrackingStore, Any, TaskRunState) -> None
    try:
        tracking_store.set_task_run_state(task_run=task_run, state=state)
    except Exception as exc:
        logger.error("Unable to set task run state: %s", exc)


def save_extrnal_links_safe(tracking_store, task_run, links_dict):
    # (TrackingStore, Any, Dict[str, str]) -> None
    try:
        tracking_store.save_external_links(
            task_run=task_run, external_links_dict=links_dict
        )
    except Exception as exc:
        logger.error("Unable to set external links: %s", exc)


@contextmanager
def track_data_proc_pyspark_operator(operator, tracking_info):
    if operator.dataproc_properties is None:
        operator.dataproc_properties = dict()
    spark_envs = add_spark_env_fields(tracking_info)
    operator.dataproc_properties.update(spark_envs)
    yield


@contextmanager
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
    yield


def _has_java_application(operator):
    return (
        operator._application.endswith(".jar")
        or operator._jars
        and operator._jars.ends_with(".jar")
    )


@contextmanager
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
    yield


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
    # type: (Dict[str, str], Any) -> Optional[ContextManager]
    """
    Wrap the operator with relevant tracking method, if found such method.
    """
    for class_name, tracking_wrapper in _EXECUTE_TRACKING.items():
        if is_instance_by_class_name(operator, class_name):
            return tracking_wrapper(operator, tracking_info)
