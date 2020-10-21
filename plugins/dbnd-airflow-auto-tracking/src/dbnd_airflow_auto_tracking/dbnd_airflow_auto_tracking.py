from airflow.plugins_manager import AirflowPlugin

from dbnd import get_dbnd_project_config
from dbnd_airflow.tracking.airflow_patching import (
    add_tracking_to_policy,
    patch_airflow_context_vars,
)


config = get_dbnd_project_config()
if config.airflow_auto_tracking:
    add_tracking_to_policy()
    patch_airflow_context_vars()


class DbndAutoTracking(AirflowPlugin):
    name = "dbnd_airflow_auto_tracking"
