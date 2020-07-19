from airflow.plugins_manager import AirflowPlugin

from dbnd._core.inplace_run.airflow_utils import (
    add_tracking_to_policy,
    patch_airflow_context_vars,
)


add_tracking_to_policy()
patch_airflow_context_vars()


class DbndAutoTracking(AirflowPlugin):
    name = "dbnd_airflow_auto_tracking"
