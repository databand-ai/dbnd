from typing import Dict, List

from dbnd import parameter
from dbnd._core.task import Config


class AirflowMonitorConfig(Config):

    _conf__task_family = "airflow_monitor"

    prometheus_port = parameter(default=8000, description="Port for airflow-monitor")[
        int
    ]

    interval = parameter(
        default=5, description="Sleep time (in seconds) between fetches when not busy"
    )[int]

    # Used by db fetcher
    local_dag_folder = parameter(default=None)[str]

    sql_alchemy_conn = parameter(default=None, description="db url", hidden=True)[str]

    rbac_username = parameter(
        default={},
        description="Username credentials to use when monitoring airflow with rbac enabled",
    )[str]

    rbac_password = parameter(
        default={},
        description="Password credentials to use when monitoring airflow with rbac enabled",
    )[str]

    # Used by file fetcher
    json_file_path = parameter(
        default=None, description="A json file to be read ExportData information from"
    )[str]

    operator_user_kwargs = parameter(
        default=[],
        description="Control which task arguments should be treated as user instead of system",
    )[Dict[str, List[str]]]

    debug_sync_log_dir_path = parameter(default=None)[str]

    allow_duplicates = parameter(default=False)[bool]

    syncer_name = parameter(default=None)[str]

    fetcher = parameter(default=None)[str]

    number_of_iterations = parameter(default=None)[int]

    stop_after = parameter(default=None)[int]

    runner_type = parameter(default="seq")[str]  # seq/mp
