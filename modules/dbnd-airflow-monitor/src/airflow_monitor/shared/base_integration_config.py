# © Copyright Databand.ai, an IBM Company 2022

from typing import Optional
from uuid import UUID

import attr

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig


DEFAULT_SYNC_INTERVAL_IN_SECONDS = 10
DEFAULT_SYNC_BULK_SIZE = 10


@attr.s(auto_attribs=True, kw_only=True)
class BaseIntegrationConfig:
    uid: UUID
    source_name: str
    source_type: str
    tracking_source_uid: UUID

    sync_interval: int = DEFAULT_SYNC_INTERVAL_IN_SECONDS
    sync_bulk_size: int = DEFAULT_SYNC_BULK_SIZE
    fetcher_type: Optional[str] = None

    log_level: str = None
    syncer_max_retries: int = 5

    credentials: str = ""
    integration_config: dict = {}

    @classmethod
    def create(cls, config: dict, monitor_config: Optional[BaseMonitorConfig] = None):
        if "integration_config" not in config:
            raise KeyError(
                f"Unable to create server config for integration with uid={config.get('uid')}, name={config.get('name')} integration_config is not found."
            )
        integration_config = config.get("integration_config")
        tracking_source_uid = None
        if "tracking_source_uid" in config:
            tracking_source_uid = config["tracking_source_uid"]
        elif len(integration_config.get("synced_entities", [])) > 0:
            tracking_source_uid = integration_config["synced_entities"][0][
                "tracking_source_uid"
            ]
        return BaseIntegrationConfig(
            uid=config["uid"],
            source_type=config["integration_type"],
            source_name=config["name"],
            credentials=config["credentials"],
            tracking_source_uid=tracking_source_uid,
            integration_config=integration_config,
            sync_interval=integration_config.get(
                "sync_interval", DEFAULT_SYNC_INTERVAL_IN_SECONDS
            ),
            # currently some existing integrations has "runs_bulk_size" field, fix it :)
            sync_bulk_size=integration_config.get(
                "runs_bulk_size", DEFAULT_SYNC_BULK_SIZE
            ),
        )
