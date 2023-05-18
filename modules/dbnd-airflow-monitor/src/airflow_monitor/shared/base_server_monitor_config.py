# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional
from uuid import UUID

import attr

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig


@attr.s(auto_attribs=True, kw_only=True)
class BaseServerConfig:
    uid: UUID
    source_name: str
    source_type: str
    tracking_source_uid: UUID

    sync_interval: int = 10  # Sync interval in seconds
    is_sync_enabled: bool = True
    fetcher_type: Optional[str] = None

    log_level: str = None
    is_generic_syncer_enabled: bool = False
    syncer_max_retries: int = 5

    @classmethod
    def create(
        cls, server_config: dict, monitor_config: Optional[BaseMonitorConfig] = None
    ):
        raise NotImplementedError()

    @property
    def identifier(self):
        return self.tracking_source_uid
