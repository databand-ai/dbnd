import logging

from airflow.plugins_manager import AirflowPlugin

from dbnd import get_dbnd_project_config
from dbnd_airflow.tracking.airflow_patching import (
    add_tracking_to_policy,
    patch_airflow_context_vars,
    patch_snowflake_hook,
)
from dbnd_airflow.tracking.dbnd_airflow_handler import set_dbnd_handler


# This code will run every time Airflow imports our plugin
# it will change current airflow policy to include Databand policy via add_tracking_to_policy
# -> `dbnd_airflow.tracking.dbnd_dag_tracking.track_task` will be applied on every task
# -> `dbnd_airflow.tracking.execute_tracking.new_execute` will wrap Operator.execute
# -> `dbnd_airflow.tracking.execute_tracking.af_tracking_context` will modify
#     airflow operator to have "our tracking parameters"
# -> `wrap_operator_with_tracking_info` will modify operator to have proper "ENV/params/spark.configs/jars.."
#     so the internal part is tracked
config = get_dbnd_project_config()
if config.airflow_auto_tracking:
    # Wraps all Operator.execute() with DBND trackign wrapper
    add_tracking_to_policy()
    # Adds retry and context uid to vars
    patch_airflow_context_vars()
    # Handler to send logs to DBND
    set_dbnd_handler()
    patch_snowflake_hook()


class DbndAutoTracking(AirflowPlugin):
    name = "dbnd_airflow_auto_tracking"
