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


class DataStageMonitorConfig(BaseMonitorConfig):
    _env_prefix = "DBND__DATASTAGE_MONITOR__"

    log_exception_to_webserver: bool = attr.ib(default=False, converter=bool)


@attr.s
class DataStageServerConfig(BaseServerConfig):
    uid = attr.ib(default=None)  # type: str
    project_id = attr.ib(default=None)  # type: str
    project_ids = attr.ib(factory=list)
    api_key = attr.ib(default=None)  # type: str
    runs_bulk_size = attr.ib(default=10)  # type: int
    page_size = attr.ib(default=200)  # type: int
    fetching_interval_in_minutes = attr.ib(default=30)  # type: int
    number_of_fetching_threads = attr.ib(default=1)  # type: int
    datastage_runs_syncer_enabled = attr.ib(default=True)  # type: bool
    host_name = attr.ib(default=None)  # type: str
    authentication_provider_url = attr.ib(default=None)  # type: str
    authentication_type = attr.ib(default=None)  # type: str
    log_level = attr.ib(default=None)  # type: str
    log_exception_to_webserver = attr.ib(default=False)  # type: bool

    @classmethod
    def create(
        cls,
        server_config: dict,
        monitor_config: Optional[DataStageMonitorConfig] = None,
    ):
        monitor_instance_config = server_config.get("monitor_config") or {}
        project_id = server_config["project_id"]
        conf = cls(
            uid=server_config.get("uid") or server_config["tracking_source_uid"],
            source_type="datastage",
            source_name=server_config["source_name"],
            tracking_source_uid=server_config["tracking_source_uid"],
            is_sync_enabled=server_config["is_sync_enabled"],
            runs_bulk_size=monitor_instance_config["runs_bulk_size"],
            page_size=monitor_instance_config["page_size"],
            fetching_interval_in_minutes=monitor_instance_config[
                "fetching_interval_in_minutes"
            ],
            number_of_fetching_threads=monitor_instance_config[
                "number_of_fetching_threads"
            ],
            project_id=project_id,
            project_ids=server_config.get("project_ids", [project_id]),
            api_key=server_config["api_key"],
            host_name=server_config["host_name"],
            authentication_provider_url=server_config["authentication_provider_url"],
            authentication_type=server_config["authentication_type"],
            sync_interval=monitor_instance_config["sync_interval"],
            datastage_runs_syncer_enabled=monitor_instance_config[
                "datastage_runs_syncer_enabled"
            ],
            log_level=monitor_instance_config.get("log_level"),
            log_exception_to_webserver=monitor_config.log_exception_to_webserver
            if monitor_config
            else False,
        )
        return conf

    @property
    def identifier(self):
        return self.uid


@attr.s(auto_attribs=True)
class DataStageMonitorState(BaseMonitorState):
    monitor_status: str = NOTHING
    monitor_error_message: Optional[str] = NOTHING
    last_sync_time: Optional[datetime.datetime] = NOTHING

    def as_dict(self) -> dict:
        data = super().as_dict()
        if data.get("last_sync_time"):
            data["last_sync_time"] = self.last_sync_time.isoformat()
        return data
