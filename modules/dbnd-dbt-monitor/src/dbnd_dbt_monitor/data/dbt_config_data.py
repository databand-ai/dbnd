# © Copyright Databand.ai, an IBM Company 2022
import datetime

from typing import Optional

import attr

from airflow_monitor.shared.base_monitor_config import (
    NOTHING,
    BaseMonitorConfig,
    BaseMonitorState,
)
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig


@attr.s
class DbtServerConfig(BaseServerConfig):
    account_id = attr.ib(default=None)  # type: int
    api_token = attr.ib(default=None)  # type: str
    job_id = attr.ib(default=None)  # type: int
    runs_bulk_size = attr.ib(default=10)  # type: int
    dbt_runs_syncer_enabled = attr.ib(default=True)  # type: bool

    @classmethod
    def create(
        cls, server_config: dict, monitor_config: Optional[BaseMonitorConfig] = None
    ):
        monitor_instance_config = server_config.get("monitor_config") or {}

        conf = cls(
            source_type="dbt",
            source_name=server_config["source_name"],
            tracking_source_uid=server_config["tracking_source_uid"],
            is_sync_enabled=server_config["is_sync_enabled"],
            runs_bulk_size=monitor_instance_config["runs_bulk_size"],
            account_id=server_config["account_id"],
            api_token=server_config["api_token"],
            sync_interval=monitor_instance_config["sync_interval"],
            job_id=monitor_instance_config.get("job_id"),
            dbt_runs_syncer_enabled=monitor_instance_config["dbt_runs_syncer_enabled"],
        )
        return conf


@attr.s(auto_attribs=True)
class DbtMonitorState(BaseMonitorState):
    monitor_status: str = NOTHING
    monitor_error_message: Optional[str] = NOTHING
    last_sync_time: Optional[datetime.datetime] = NOTHING

    def as_dict(self) -> dict:
        data = super().as_dict()
        if data.get("last_sync_time"):
            data["last_sync_time"] = self.last_sync_time.isoformat()
        return data


class DbtMonitorConfig(BaseMonitorConfig):
    _env_prefix = "DBND__DBT_MONITOR__"
