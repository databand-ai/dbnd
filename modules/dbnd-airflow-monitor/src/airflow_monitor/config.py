# © Copyright Databand.ai, an IBM Company 2022
from typing import Optional

import attr

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig


def to_bool(value):
    if isinstance(value, bool):
        return bool(value)
    value = value.lower()
    if value in ("true", "t", "1", "yes", "y"):
        return True
    if value in ("false", "f", "0", "no", "n"):
        return False
    raise ValueError(f"Cannot convert value to bool: {value}")


@attr.s(auto_attribs=True)
class AirflowMonitorConfig(BaseMonitorConfig):
    _env_prefix = "DBND__AIRFLOW_MONITOR__"

    # Used by db fetcher. Default was in databand.cfg (TODO: it's not used, should be removed)
    local_dag_folder: str = "/usr/local/airflow/dags"

    # Set which database URL will be used.
    sql_alchemy_conn: Optional[str] = None

    # Set which username credentials will be used when monitoring airflow with rbac enabled.
    #  Default was in databand.cfg
    rbac_username: str = "databand"

    # Set which password credentials will be used when monitoring airflow with rbac enabled.
    #  Default was in databand.cfg
    rbac_password: str = "databand"

    # Used by file fetcher
    # Set the path to the JSON file from which ExportData information will be read.
    json_file_path: Optional[str] = None

    fetcher: Optional[str] = None

    # Syncer is enabled and syncing
    is_sync_enabled: bool = attr.ib(converter=to_bool, default=True)
