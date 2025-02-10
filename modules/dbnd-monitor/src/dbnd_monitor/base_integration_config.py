# © Copyright Databand.ai, an IBM Company 2022


from typing import Optional
from uuid import UUID

import attr

from dbnd_monitor.base_monitor_config import BaseMonitorConfig


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

    credentials: str = ""
    integration_config: dict = {}

    component_error_support: Optional[bool] = False

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
        component_error_support = False
        if (
            monitor_config
            and monitor_config.component_error_support
            and config.get("component_error_support", False)
        ):
            component_error_support = True

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
            component_error_support=component_error_support,
        )
