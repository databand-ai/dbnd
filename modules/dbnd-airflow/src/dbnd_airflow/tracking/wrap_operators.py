# © Copyright Databand.ai, an IBM Company 2022

import logging

from collections import OrderedDict
from typing import Any, ContextManager, Dict, Optional

from dbnd._core.utils.basics.load_python_module import load_python_callable
from dbnd_airflow.tracking.dbnd_spark_conf import (
    track_databricks_submit_run_operator,
    track_dataproc_pyspark_operator,
    track_dataproc_submit_job_operator,
    track_ecs_operator,
    track_emr_add_steps_operator,
    track_spark_submit_operator,
)


logger = logging.getLogger(__name__)

# registering operators names to the relevant tracking method
_EXECUTE_TRACKING = OrderedDict(
    [
        # Airflow 1.x
        (
            "airflow.contrib.operators.emr_add_steps_operator.EmrAddStepsOperator",
            track_emr_add_steps_operator,
        ),
        (
            "airflow.contrib.operators.databricks_operator.DatabricksSubmitRunOperator",
            track_databricks_submit_run_operator,
        ),
        (
            "airflow.contrib.operators.dataproc_operator.DataProcPySparkOperator",
            track_dataproc_pyspark_operator,
        ),
        (
            "airflow.contrib.operators.spark_submit_operator.SparkSubmitOperator",
            track_spark_submit_operator,
        ),
        ("airflow.contrib.operators.ecs_operator.ECSOperator", track_ecs_operator),
        # Airflow 2.x
        (
            "airflow.providers.amazon.aws.operators.emr.EmrAddStepsOperator",
            track_emr_add_steps_operator,
        ),
        (
            "airflow.providers.amazon.aws.operators.emr_pyspark.EmrPySparkOperator",
            track_emr_add_steps_operator,
        ),
        (
            "airflow.providers.amazon.aws.operators.emr_create_job_flow.EmrCreateJobFlowOperator",
            track_emr_add_steps_operator,
        ),
        (
            "airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator",
            track_databricks_submit_run_operator,
        ),
        (
            "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator",
            track_dataproc_submit_job_operator,
        ),
        (
            "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator",
            track_dataproc_pyspark_operator,
        ),
        (
            "airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator",
            track_spark_submit_operator,
        ),
        ("airflow.providers.amazon.aws.operators.ecs.ECSOperator", track_ecs_operator),
        # Airflow 2.x - with lowercase Ecs (not ECS)
        ("airflow.providers.amazon.aws.operators.ecs.EcsOperator", track_ecs_operator),
    ]
)


def register_airflow_operator_handler(operator, airflow_operator_handler):
    logger.debug(
        "Registering operator handler %s with %s", operator, airflow_operator_handler
    )
    global _EXECUTE_TRACKING
    if isinstance(operator, type):
        operator = f"{operator.__module__}.{operator.__qualname__}"
    _EXECUTE_TRACKING[operator] = airflow_operator_handler


def get_airflow_operator_handlers_config(user_config_airflow_operator_handlers=None):
    if user_config_airflow_operator_handlers:
        target = _EXECUTE_TRACKING.copy()
        target.update(user_config_airflow_operator_handlers)
        return target

    return _EXECUTE_TRACKING


def wrap_operator_with_tracking_info(
    tracking_info: Dict[str, str],
    operator: Any,
    airflow_operator_handlers: Dict[str, Any],
) -> Optional[ContextManager]:
    """
    Wrap the operator with relevant tracking method, if found such method.
    """
    for cls in operator.__class__.mro():
        # for user operators we support only FQN matching
        tracking_wrapper = _get_loaded_tracking_wrapper(
            airflow_operator_handlers, cls.__qualname__
        ) or _get_loaded_tracking_wrapper(
            airflow_operator_handlers, f"{cls.__module__}.{cls.__qualname__}"
        )
        if tracking_wrapper:
            return tracking_wrapper(operator, tracking_info)
    return None


def _get_loaded_tracking_wrapper(airflow_operator_handlers, name):
    if name not in airflow_operator_handlers:
        return None

    tracking_wrapper = airflow_operator_handlers.get(name)
    if isinstance(tracking_wrapper, str):
        tracking_wrapper = load_python_callable(tracking_wrapper)
        # Update the cache with the loaded function
        airflow_operator_handlers[name] = tracking_wrapper
    return tracking_wrapper
