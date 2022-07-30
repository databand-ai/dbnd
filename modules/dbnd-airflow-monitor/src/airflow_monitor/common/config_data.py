# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional

import attr

from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig


# Do not change this name unless you change the same constant in constants.py in dbnd-airflow
MONITOR_DAG_NAME = "databand_airflow_monitor"


@attr.s
class AirflowServerConfig(BaseServerConfig):
    state_sync_enabled = attr.ib(default=False)  # type: bool
    xcom_sync_enabled = attr.ib(default=False)  # type: bool
    dag_sync_enabled = attr.ib(default=False)  # type: bool
    fixer_enabled = attr.ib(default=False)  # type: bool
    config_updater_enabled = attr.ib(default=False)  # type: bool
    include_sources = attr.ib(default=False)  # type: bool

    base_url = attr.ib(default=None)  # type: str
    api_mode = attr.ib(default=None)  # type: str
    fetcher_type = attr.ib(default="web")  # type: str
    dag_ids = attr.ib(default=None)  # type: Optional[str]

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
    sync_interval = attr.ib(default=10)  # type: int

    # runtime fixer config
    fix_interval = attr.ib(default=600)  # type: int

    # runtime config updater config
    config_updater_interval = attr.ib(default=60)  # type: int

    @classmethod
    def create(
        cls, server_config: dict, monitor_config: Optional[BaseMonitorConfig] = None
    ):
        if monitor_config is None:
            monitor_config = AirflowMonitorConfig()

        monitor_instance_config = server_config.get("monitor_config") or {}
        kwargs = {
            k: v
            for k, v in monitor_instance_config.items()
            if k in attr.fields_dict(cls)
        }

        dag_ids = server_config["dag_ids"]
        if dag_ids:
            dag_ids = dag_ids + "," + MONITOR_DAG_NAME

        conf = cls(
            source_type="airflow",
            source_name=server_config["name"],
            tracking_source_uid=server_config["tracking_source_uid"],
            is_sync_enabled=server_config["is_sync_enabled"],
            base_url=server_config["base_url"],
            api_mode=server_config["api_mode"],
            fetcher_type=monitor_config.fetcher or server_config["fetcher"],
            composer_client_id=server_config["composer_client_id"],
            dag_ids=dag_ids,
            sql_alchemy_conn=monitor_config.sql_alchemy_conn,  # TODO: currently support only one server!
            json_file_path=monitor_config.json_file_path,  # TODO: currently support only one server!
            rbac_username=monitor_config.rbac_username,  # TODO: currently support only one server!
            rbac_password=monitor_config.rbac_password,  # TODO: currently support only one server!
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
