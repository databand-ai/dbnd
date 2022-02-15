from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from dbnd import parameter


class AirflowMonitorConfig(BaseMonitorConfig):

    _conf__task_family = "airflow_monitor"

    # Used by db fetcher
    local_dag_folder = parameter(default=None)[str]

    sql_alchemy_conn = parameter(default=None, description="db url", hidden=True)[str]

    rbac_username = parameter(
        default={},
        description="Username credentials to use when monitoring airflow with rbac enabled",
    )[str]

    syncer_name = parameter(default=None)[str]

    rbac_password = parameter(
        default={},
        description="Password credentials to use when monitoring airflow with rbac enabled",
    )[str]

    # Used by file fetcher
    json_file_path = parameter(
        default=None, description="A json file to be read ExportData information from"
    )[str]

    fetcher = parameter(default=None)[str]
