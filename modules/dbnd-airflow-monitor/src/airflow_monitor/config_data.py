# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional

import attr

from airflow_monitor.cli.config import AirflowMonitorConfig
from dbnd_monitor.base_integration_config import BaseIntegrationConfig
from dbnd_monitor.base_monitor_config import BaseMonitorConfig


# Do not change this name unless you change the same constant in compat.py in dbnd-airflow
MONITOR_DAG_NAME = "databand_airflow_monitor"


@attr.s
class AirflowIntegrationConfig(BaseIntegrationConfig):
    include_sources = attr.ib(default=False)  # type: bool

    dag_ids = attr.ib(default=None)  # type: Optional[str]
    excluded_dag_ids = attr.ib(default=None)  # type: Optional[str]

    # for db data fetcher
    local_dag_folder = attr.ib(default=None)  # type: str
    sql_alchemy_conn = attr.ib(default=None)  # type: str

    # runtime syncer config
    dag_run_bulk_size = attr.ib(default=10)  # type: int

    # runtime config updater config
    config_updater_interval = attr.ib(default=60)  # type: int

    # force restart after failed to sync for X minutes
    restart_after_not_synced_minutes = attr.ib(default=5)  # type: int

    @classmethod
    def create(cls, config: dict, monitor_config: Optional[BaseMonitorConfig] = None):
        if monitor_config is None:
            monitor_config = AirflowMonitorConfig.from_env()

        monitor_instance_config = config.get("monitor_config") or {}
        kwargs = {
            k: v
            for k, v in monitor_instance_config.items()
            if k in attr.fields_dict(cls)
        }

        dag_ids = config["dag_ids"]
        if dag_ids:
            dag_ids = dag_ids + "," + MONITOR_DAG_NAME
        excluded_dag_ids = config.get("excluded_dag_ids")

        conf = cls(
            uid=config["uid"],
            source_type="airflow",
            source_name=config["name"],
            tracking_source_uid=config["tracking_source_uid"],
            fetcher_type=monitor_config.fetcher or config["fetcher"],
            dag_ids=dag_ids,
            excluded_dag_ids=excluded_dag_ids,
            sql_alchemy_conn=monitor_config.sql_alchemy_conn,
            **kwargs,
        )
        return conf
