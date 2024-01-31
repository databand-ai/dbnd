# © Copyright Databand.ai, an IBM Company 2022

import logging
import os
import typing

from contextlib import contextmanager
from typing import Any, Dict, List

import six

from dbnd import dbnd_context
from dbnd._core.configuration.environ_config import (
    ENV_DBND__ENABLE__SPARK_CONTEXT_ENV,
    ENV_DBND_SCRIPT_NAME,
)
from dbnd._core.constants import TaskRunState, UpdateSource
from dbnd._core.log import dbnd_log_exception, is_verbose
from dbnd._core.log.dbnd_log import ENV_DBND__VERBOSE, dbnd_log_debug, dbnd_log_info
from dbnd._core.tracking.airflow_dag_inplace_tracking import (
    get_task_run_uid_for_inline_script,
)
from dbnd._core.tracking.backends import TrackingStore
from dbnd_airflow.tracking.config import TrackingSparkConfig
from dbnd_airflow.tracking.fakes import FakeRun, FakeTask, FakeTaskRun


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from airflow.contrib.operators.databricks_operator import (
        DatabricksHook,
        DatabricksSubmitRunOperator,
    )
    from airflow.contrib.operators.ecs_operator import ECSOperator
    from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

logger = logging.getLogger(__name__)


def convert_spark_conf_to_cli_args(conf):
    """Flats a configuration iterable to a list of commands ready to concat to"""
    results = []
    for field, value in six.iteritems(conf):
        results.extend(["--conf", field + "=" + value])
    return results


def _merge_jars(jar_uris, spark_jars):
    # type: (List[str], str) -> List[str]
    """
    Merge jar list, but for jar_file_uris property. spark_jars is comma-separated list of jars
    """
    if spark_jars:
        return jar_uris + spark_jars.split(",")
    return jar_uris


def _normalize_python_script_name(raw_script_path: str) -> typing.Optional[str]:
    if not raw_script_path:
        return None
    try:
        script_name = os.path.basename(raw_script_path)
        if not script_name:
            logger.warning(
                "Unable to determine script name from path %s", raw_script_path
            )
            return None

        return script_name
    except Exception as exc:
        logger.error(
            "Unable to determine script name from path %s, exception: %s",
            raw_script_path,
            exc,
        )
        return None


def get_dbnd_context_spark_conf(
    tracking_info: Dict[str, str], spark_conf: Dict[str, str] = None
) -> Dict[str, str]:
    """
    Takes dbnd context as dict (airflow context + databand task context), builds spark.env properties,
    and enriches it with properties required for proper tracking in client and cluster modes
    """

    #     if try_get_current_task_run():
    #         tracking_info = get_tracking_information(context, try_get_current_task_run())

    dbnd_spark_conf = {
        "spark.env." + key: value for key, value in tracking_info.items()
    }
    dbnd_spark_conf.update(
        {
            # Spark properties to explicitly enable tracking
            "spark.env.DBND__TRACKING": "True",
            # Spark properties to explicitly enable tracking in cluster mode
            "spark.yarn.appMasterEnv.DBND__TRACKING": "True",
            # might be missing in server mode
            "spark.yarn.appMasterEnv.SPARK_ENV_LOADED": "1",
            "spark.yarn.appMasterEnv.DBND__ENABLE__SPARK_CONTEXT_ENV": "True",
            "spark.yarn.appMasterEnv.DBND_HOME": "/tmp/dbnd",
            # Spark properties to explicitly enable tracking in K8S mode
            "spark.kubernetes.driverEnv.DBND__TRACKING": "True",
            "spark.kubernetes.driverEnv.DBND__ENABLE__SPARK_CONTEXT_ENV": "True",
        }
    )

    if is_verbose():
        dbnd_spark_conf.update(
            {
                "spark.env.DBND__VERBOSE": "True",
                "spark.yarn.appMasterEnv.DBND__VERBOSE": "True",
                "spark.kubernetes.driverEnv.DBND__VERBOSE": "True",
            }
        )

    dbnd_spark_conf.update(TrackingSparkConfig.from_databand_context().spark_conf())

    # simple case, used doesn't have his own configuration
    if not spark_conf:
        dbnd_log_debug("Injecting Env variables %s", dbnd_spark_conf)
        return dbnd_spark_conf

    # we need to merge
    merged_conf = spark_conf.copy()
    merged_conf.update(dbnd_spark_conf)

    # Operator spark conf can contain "spark.jars", "spark.driver.extraJavaOptions"
    # and "spark.sql.queryExecutionListeners".
    # To preserve existing properties we need to concat them with patched properties
    for property_name in [
        "spark.driver.extraJavaOptions",
        "spark.jars",
        "spark.sql.queryExecutionListeners",
    ]:
        if property_name in spark_conf and property_name in dbnd_spark_conf:
            merged_conf[property_name] = (
                spark_conf[property_name] + "," + dbnd_spark_conf[property_name]
            )

    dbnd_log_debug("Injecting Env variables with merge: %s", dbnd_spark_conf)
    return merged_conf


def get_dbnd_context_env_vars(tracking_info=None, env_vars=None, script_name=None):
    env_vars_with_dbnd = env_vars.copy() if env_vars else dict()
    env_vars_with_dbnd.update(tracking_info)
    # env_vars_with_dbnd.update(get_databand_url_conf())
    if script_name:
        script_name = _normalize_python_script_name(script_name)
        if script_name:
            env_vars_with_dbnd[ENV_DBND_SCRIPT_NAME] = script_name

    if is_verbose():
        env_vars_with_dbnd[ENV_DBND__VERBOSE] = "True"
    env_vars_with_dbnd[ENV_DBND__ENABLE__SPARK_CONTEXT_ENV] = "True"
    return env_vars_with_dbnd


def get_spark_submit_cli_with_dbnd_context(
    original_command_as_list, dbnd_context_as_cli=None
):
    """This functions augments spark-submit command with dbnd tracking metadata aka dbnd_context_as_cli. If context
    is not provided, a default Airflow templates are set for DAG Id, Task Id, and execution date

    Adds 3 configuration properties to spark-submit to associate spark run with airflow DAG that initiated this run.
    These properties are
        spark.env.AIRFLOW_CTX_DAG_ID  -  name of the Airflow DAG to associate a run with
        spark.env.AIRFLOW_CTX_EXECUTION_DATE - execution_date to associate a run with
        spark.env.AIRFLOW_CTX_TASK_ID" - name of the Airflow Task to associate a run with

    Example:
        >>> get_spark_submit_cli_with_dbnd_context(["spark-submit","my_script.py","my_param"])
        ['spark-submit', '--conf', 'spark.env.AIRFLOW_CTX_DAG_ID={{dag.dag_id}}', '--conf', 'spark.env.AIRFLOW_CTX_EXECUTION_DATE={{ts}}', '--conf', 'spark.env.AIRFLOW_CTX_TASK_ID={{task.task_id}}', 'my_script.py', 'my_param']

    Args:
        original_command_as_list: spark-submit command line as a list of strings.
        dbnd_context_as_cli: optional dbnd_context provided by a user. I

    Returns:
        An updated spark-submit command including dbnd context.

    """
    index = next(
        (
            original_command_as_list.index(elm)
            for elm in original_command_as_list
            if "spark-submit" in elm
        ),
        -1,
    )
    if index == -1:
        dbnd_log_exception(
            "Failed to find 'spark-submit' cmd in %s"
            % " ".join(original_command_as_list)
        )
        return original_command_as_list

    return (
        original_command_as_list[0 : index + 1]
        + dbnd_context_as_cli
        + original_command_as_list[index + 1 :]
    )


@contextmanager
def track_emr_add_steps_operator(operator, tracking_info):
    dbnd_log_info("Updating HadoopJarStep.Args with dbnd context")

    spark_envs = get_dbnd_context_spark_conf(tracking_info)
    # EMR uses list arguments, it's safe to update it

    dbnd_context_as_cli = convert_spark_conf_to_cli_args(spark_envs)
    for step in operator.steps:
        args = step["HadoopJarStep"]["Args"]
        if args and "spark-submit" in args[0]:
            # Add dbnd and airflow context
            step["HadoopJarStep"]["Args"] = get_spark_submit_cli_with_dbnd_context(
                args, dbnd_context_as_cli=dbnd_context_as_cli
            )

        else:
            dbnd_log_exception("spark-submit has been not found in the operator")
    yield


@contextmanager
def track_pyspark_operator_with_spark_args_and_jars_attrs(operator, tracking_info):
    if operator.spark_args is None:
        operator.spark_args = []

    spark_args_array = convert_spark_conf_to_cli_args(
        get_dbnd_context_spark_conf(tracking_info)
    )
    operator.spark_args.extend(spark_args_array)
    yield


@contextmanager
def track_databricks_submit_run_operator(operator, tracking_info):
    # type: (DatabricksSubmitRunOperator, Dict[str, str])-> None
    config = operator.json
    script_name = None  # type: str
    # passing env variables is only supported in new clusters
    cluster = None

    if "new_cluster" in config:
        cluster = config["new_cluster"]
        cluster.setdefault("spark_env_vars", {})
        # via Environment
        cluster["spark_env_vars"] = get_dbnd_context_env_vars(
            tracking_info=tracking_info,
            script_name=config.get("spark_python_task", {}).get("python_file"),
            env_vars=cluster.get("spark_env_vars"),
        )

        # via spark.conf
        cluster["spark_conf"] = get_dbnd_context_spark_conf(
            tracking_info=tracking_info, spark_conf=cluster.get("spark_conf")
        )

        if "spark_python_task" in config:
            script_name = cluster["spark_env_vars"].get(ENV_DBND_SCRIPT_NAME)

    yield
    if script_name:
        # calculate deterministic task uids so we can use it for manual completion
        (
            task_id,
            task_run_uid,
            task_run_attempt_uid,
        ) = get_task_run_uid_for_inline_script(tracking_info, script_name)

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
    spark_envs = get_dbnd_context_spark_conf(
        tracking_info, spark_conf=operator.dataproc_properties
    )

    # It's possible to pass JARs to Dataproc operator in two ways:
    # 1. `spark.jars` property
    # 2. `dataproc_jars` property
    # Those properties are mutually exclude each other
    # So if dataproc_jars are populated we're gonna add our jar here
    if operator.dataproc_jars and len(operator.dataproc_jars) > 0:
        # del spark.jars from spark props because jars are in `jar_file_uris` property already
        operator.dataproc_jars = _merge_jars(
            operator.dataproc_jars, spark_envs.pop("spark.jars", None)
        )

    operator.dataproc_properties.update(spark_envs)
    yield


@contextmanager
def track_dataproc_submit_job_operator(operator, tracking_info):
    logger.info("Tracking DataprocSubmitJobOperator")
    if operator.job is not None:
        if "pyspark_job" in operator.job:
            logger.info("Updating properties for %s", operator)

            pyspark_job = operator.job["pyspark_job"]
            pyspark_job.setdefault("properties", {})

            spark_envs = get_dbnd_context_spark_conf(
                tracking_info=tracking_info, spark_conf=pyspark_job["properties"]
            )

            if "jar_file_uris" in pyspark_job and len(pyspark_job["jar_file_uris"]) > 0:
                # merge `jar_file_uris` property
                pyspark_job["jar_file_uris"] = _merge_jars(
                    pyspark_job["jar_file_uris"], spark_envs.pop("spark.jars", None)
                )

            pyspark_job["properties"].update(spark_envs)
            logger.info(
                "Properties for pyspark_job was updated for task %s", operator.task_id
            )
        else:
            logger.info("pyspark_job has been not found in the operator")
    yield


@contextmanager
def track_spark_submit_operator(operator, tracking_info):
    # type: (SparkSubmitOperator, Dict[str,str])-> None
    if operator._conf is None:
        operator._conf = dict()
    operator._conf = get_dbnd_context_spark_conf(
        tracking_info, spark_conf=operator._conf
    )
    operator._env_vars = get_dbnd_context_env_vars(
        tracking_info, env_vars=operator._env_vars
    )

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


@contextmanager
def track_custom_spark_submit_operator(operator, tracking_info):
    # CHECK that your operator doesn't have any other spark.jars
    # enables DbndSparkQueryExecutionListener. - it can be omitted as a first step

    try:
        # UPDATE CUSTOM OPERATOR
        custom_cli = convert_spark_conf_to_cli_args(
            get_dbnd_context_spark_conf(tracking_info)
        )

        if not hasattr(operator, "spark_args"):
            dbnd_log_exception(
                "User Spark Operator doesn't have attribute 'spark_args', "
                "skipping the context injection"
            )
        else:
            if operator.spark_args is None:
                operator.spark_args = []
            operator.spark_args.extend(custom_cli)

    except Exception as ex:
        dbnd_log_exception("Failed to wrap '%s'. Got an error '%s'", operator, ex)

    yield
