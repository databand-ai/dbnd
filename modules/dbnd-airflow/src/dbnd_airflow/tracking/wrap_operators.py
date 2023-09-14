# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from collections import OrderedDict
from contextlib import contextmanager
from typing import Any, ContextManager, Dict, Optional

from dbnd._core.log import dbnd_log_debug
from dbnd._core.utils.basics.load_python_module import load_python_callable
from dbnd_airflow.tracking.dbnd_spark_conf import (
    track_databricks_submit_run_operator,
    track_dataproc_pyspark_operator,
    track_dataproc_submit_job_operator,
    track_ecs_operator,
    track_emr_add_steps_operator,
    track_spark_submit_operator,
)


if typing.TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator

logger = logging.getLogger(__name__)

# registering operators names to the relevant tracking method
_EXECUTE_TRACKING = OrderedDict(
    [
        # SPARK SUBMIT
        # Airflow 1.x
        (
            "airflow.contrib.operators.spark_submit_operator.SparkSubmitOperator",
            track_spark_submit_operator,
        ),
        # Airflow 2.x
        (
            "airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator",
            track_spark_submit_operator,
        ),
        # EMR
        # Airflow 1.x
        (
            "airflow.contrib.operators.emr_add_steps_operator.EmrAddStepsOperator",
            track_emr_add_steps_operator,
        ),
        # Airflow 2.0
        (
            "airflow.providers.amazon.aws.operators.emr_add_steps.EmrAddStepsOperator",
            track_emr_add_steps_operator,
        ),
        (
            "airflow.providers.amazon.aws.operators.emr_pyspark.EmrPySparkOperator",
            track_emr_add_steps_operator,
        ),
        # Airflow 2.2+
        (
            "airflow.providers.amazon.aws.operators.emr.EmrAddStepsOperator",
            track_emr_add_steps_operator,
        ),
        # Databricks - track_databricks_submit_run_operator
        # Airflow 1.x
        (
            "airflow.contrib.operators.databricks_operator.DatabricksSubmitRunOperator",
            track_databricks_submit_run_operator,
        ),
        # Airflow 2.x
        (
            "airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator",
            track_databricks_submit_run_operator,
        ),
        # Dataproc - track_dataproc_submit_job_operator
        # Airflow 1.x
        (
            "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator",
            track_dataproc_submit_job_operator,
        ),
        # Airflow 2.x
        (
            "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator",
            track_dataproc_pyspark_operator,
        ),
        # ECS
        # Airflow 1.x -
        ("airflow.contrib.operators.ecs_operator.ECSOperator", track_ecs_operator),
        # # Airflow 2.x
        ("airflow.providers.amazon.aws.operators.ecs.ECSOperator", track_ecs_operator),
        # Airflow 2.x - with lowercase Ecs (not ECS)
        ("airflow.providers.amazon.aws.operators.ecs.EcsOperator", track_ecs_operator),
    ]
)


def register_airflow_operator_handler(operator, airflow_operator_handler):
    global _EXECUTE_TRACKING
    if isinstance(operator, type):
        operator = f"{operator.__module__}.{operator.__qualname__}"

    dbnd_log_debug(
        "Registering operator handler %s with %s", operator, airflow_operator_handler
    )
    _EXECUTE_TRACKING[operator] = airflow_operator_handler


def get_airflow_operator_handlers_config(user_config_airflow_operator_handlers=None):
    if user_config_airflow_operator_handlers:
        target = _EXECUTE_TRACKING.copy()
        target.update(user_config_airflow_operator_handlers)
        return target

    return _EXECUTE_TRACKING


def wrap_operator_with_tracking_info(
    tracking_info: Dict[str, str],
    operator: "BaseOperator",
    airflow_operator_handlers: Dict[str, Any],
) -> Optional[ContextManager]:
    """
    Wrap the operator with relevant tracking method, if found such method.
    """

    dbnd_log_debug(
        "Checking for the relevant wrapper for operator '%s' (%s)",
        operator,
        f"{operator.__class__.__module__}.{operator.__class__.__qualname__}",
    )
    for cls in operator.__class__.mro():
        # for user operators we support only FQN matching
        tracking_wrapper = _get_loaded_tracking_wrapper(
            airflow_operator_handlers, cls.__qualname__
        ) or _get_loaded_tracking_wrapper(
            airflow_operator_handlers, f"{cls.__module__}.{cls.__qualname__}"
        )
        if tracking_wrapper:
            dbnd_log_debug(
                "Applying airflow operator wrapper %s at %s",
                operator.task_id,
                tracking_wrapper,
            )
            return tracking_wrapper(operator, tracking_info)
    dbnd_log_debug("There is no specific wrapper for '%s'", operator)
    return None


def _get_loaded_tracking_wrapper(airflow_operator_handlers, name):
    if name not in airflow_operator_handlers:
        # not found
        return None

    tracking_wrapper = airflow_operator_handlers.get(name)
    if not tracking_wrapper:
        # found but empty -> user has disabled the operator
        return None
    if isinstance(tracking_wrapper, str):
        tracking_wrapper = load_python_callable(tracking_wrapper)
        # Update the cache with the loaded function
        airflow_operator_handlers[name] = tracking_wrapper
    return tracking_wrapper


@contextmanager
def track_nope_wrapper(operator, tracking_info):
    yield
