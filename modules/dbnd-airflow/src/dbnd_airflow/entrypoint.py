# © Copyright Databand.ai, an IBM Company 2022
import logging
import os

import dbnd

from dbnd._core.log import dbnd_log_debug, is_verbose
from dbnd_airflow.tracking.airflow_patching import (
    add_tracking_to_policy,
    patch_airflow_context_vars,
    patch_snowflake_hook,
)
from dbnd_airflow.tracking.dbnd_airflow_handler import (
    AIRFLOW_TASK_LOGGER,
    add_dbnd_log_handler,
)


_airflow_bootstrap_applied = False


def dbnd_airflow_tracking_entrypoint():
    """
    all relevant patches for airflow execution
    """
    global _airflow_bootstrap_applied
    if _airflow_bootstrap_applied:
        return
    _airflow_bootstrap_applied = True  # prevent recursive call/repeated error call

    airflow_logger = logging.getLogger(AIRFLOW_TASK_LOGGER)
    dbnd_log_debug("Airflow wrapping via entrypoint")
    # This code will run every time Airflow imports our plugin
    # it will change current airflow policy to include Databand policy via add_tracking_to_policy
    # -> `dbnd_airflow.tracking.dbnd_dag_tracking.track_task` will be applied on every task
    # -> `dbnd_airflow.tracking.execute_tracking.new_execute` will wrap Operator.execute
    # new_execute -> `dbnd_airflow.tracking.execute_tracking.af_tracking_context` will modify
    #     airflow operator to have "our tracking parameters"
    # new_execute -> `wrap_operator_with_tracking_info` will modify operator to have proper
    #         "ENV/params/spark.configs/jars.." so the internal part is tracked

    # Add policy, that will wrap all Operator.execute() with DBND tracking wrapper
    add_tracking_to_policy()

    # Adds extra DBND_ variables to vars
    patch_airflow_context_vars()

    # Add handler Handler to send logs to DBND
    add_dbnd_log_handler()

    # patch snoflake if available
    patch_snowflake_hook()

    # tracking msg
    if is_verbose():
        dbnd_log_debug("Airflow wrapping has finished (pid=%s)" % os.getpid())

    airflow_logger.info(
        "DBND Tracking Entrypoint {version}".format(version=dbnd.__version__)
    )
