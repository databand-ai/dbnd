from typing import List, Optional
from uuid import UUID

import attr

from airflow_monitor.config import AirflowMonitorConfig


@attr.s
class AirflowServerConfig(object):
    tracking_source_uid = attr.ib()  # type: UUID
    name = attr.ib(default=None)  # type: str
    state_sync_enabled = attr.ib(default=False)  # type: bool
    xcom_sync_enabled = attr.ib(default=False)  # type: bool
    dag_sync_enabled = attr.ib(default=False)  # type: bool
    fixer_enabled = attr.ib(default=False)  # type: bool
    include_sources = attr.ib(default=False)  # type: bool

    is_sync_enabled = attr.ib(default=True)  # type: bool
    is_sync_enabled_v2 = attr.ib(default=False)  # type: bool
    base_url = attr.ib(default=None)  # type: str
    api_mode = attr.ib(default=None)  # type: str
    fetcher = attr.ib(default="web")  # type: str
    dag_ids = attr.ib(default=None)  # type: Optional[str]

    log_level = attr.ib(default=None)  # type: str

    # for web data fetcher
    rbac_username = attr.ib(default=None)  # type: str
    rbac_password = attr.ib(default=None)  # type: str

    # for composer data fetcher
    composer_client_id = attr.ib(default=None)  # type: str

    # for db data fetcher
    local_dag_folder = attr.ib(default=None)  # type: str
    sql_alchemy_conn = attr.ib(default=None)  # type: str

    # for file data fetcher
    json_file_path = attr.ib(default=None)  # type: str

    # runtime syncer config
    dag_run_bulk_size = attr.ib(default=10)  # type: int

    start_time_window = attr.ib(default=14)  # type: int
    interval = attr.ib(default=10)  # type: int

    # runtime fixer config
    fix_interval = attr.ib(default=600)  # type: int

    @classmethod
    def create(cls, airflow_config: AirflowMonitorConfig, server_config):
        monitor_config = server_config.get("monitor_config") or {}
        kwargs = {k: v for k, v in monitor_config.items() if k in attr.fields_dict(cls)}

        conf = cls(
            tracking_source_uid=server_config["tracking_source_uid"],
            name=server_config["name"],
            is_sync_enabled=server_config["is_sync_enabled"],
            is_sync_enabled_v2=server_config.get("is_sync_enabled_v2", False),
            base_url=server_config["base_url"],
            api_mode=server_config["api_mode"],
            fetcher=airflow_config.fetcher or server_config["fetcher"],
            composer_client_id=server_config["composer_client_id"],
            dag_ids=server_config["dag_ids"],
            sql_alchemy_conn=airflow_config.sql_alchemy_conn,  # TODO: currently support only one server!
            json_file_path=airflow_config.json_file_path,  # TODO: currently support only one server!
            rbac_username=airflow_config.rbac_username,  # TODO: currently support only one server!
            rbac_password=airflow_config.rbac_password,  # TODO: currently support only one server!
            **kwargs,
        )
        return conf


@attr.s
class TrackingServiceConfig:
    url = attr.ib()
    access_token = attr.ib(default=None)
    user = attr.ib(default=None)
    password = attr.ib(default=None)
    service_type = attr.ib(default=None)
