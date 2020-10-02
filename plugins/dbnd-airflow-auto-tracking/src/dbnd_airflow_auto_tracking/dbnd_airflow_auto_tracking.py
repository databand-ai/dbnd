from airflow.plugins_manager import AirflowPlugin

from dbnd_airflow.tracking.airflow_patching import (
    add_tracking_to_policy,
    patch_airflow_context_vars,
)


add_tracking_to_policy()
patch_airflow_context_vars()


class DbndAutoTracking(AirflowPlugin):
    name = "dbnd_airflow_auto_tracking"
