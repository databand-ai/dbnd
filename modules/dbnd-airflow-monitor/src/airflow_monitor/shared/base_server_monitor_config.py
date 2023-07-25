# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional
from uuid import UUID

import attr

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig


DEFAULT_SYNC_INTERVAL_IN_SECONDS = 10


@attr.s(auto_attribs=True, kw_only=True)
class BaseServerConfig:
    uid: UUID
    source_name: str
    source_type: str
    tracking_source_uid: UUID

    sync_interval: int = DEFAULT_SYNC_INTERVAL_IN_SECONDS
    fetcher_type: Optional[str] = None

    log_level: str = None
    syncer_max_retries: int = 5

    credentials: str = ""
    integration_config: dict = {}

    @classmethod
    def create(
        cls, server_config: dict, monitor_config: Optional[BaseMonitorConfig] = None
    ):
        if "integration_config" not in server_config:
            raise KeyError(
                f"Unable to create server config for integration with uid={server_config.get('uid')}, name={server_config.get('name')} integration_config is not found."
            )
        integration_config = server_config.get("integration_config")

        return BaseServerConfig(
            uid=server_config["uid"],
            source_type=server_config["integration_type"],
            source_name=server_config["name"],
            credentials=server_config["credentials"],
            tracking_source_uid=server_config["tracking_source_uid"],
            integration_config=integration_config,
            sync_interval=integration_config.get(
                "sync_interval", DEFAULT_SYNC_INTERVAL_IN_SECONDS
            ),
        )

    @property
    def identifier(self):
        return self.tracking_source_uid
