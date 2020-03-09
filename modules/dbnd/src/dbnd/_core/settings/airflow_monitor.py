from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import Config


class AirflowMonitorConfig(Config):
    """(Syncer) - Configuration related to airflow syncing and monitor mechanism"""

    _conf__task_family = "airflow_monitor"

    fetcher = parameter(
        default="web",
        description="Controls fetching mechanism from airflow server (web\db\composer).",
    )[str]
    interval = parameter(
        default=10, description="Controls sleeping time between each fetching cycle."
    )[int]
    include_logs = parameter(
        default=True,
        description="Whether to include dag runs and task instances logs when fetching data from airflow.",
    )[bool]
    fetch_period = parameter(
        default=60, description="Defines the end_time boundary in minutes."
    )[int]
    airflow_external_url = parameter(
        default=None,
        description="If provided, airflow base_url in server info will use this field.",
    )[str]
