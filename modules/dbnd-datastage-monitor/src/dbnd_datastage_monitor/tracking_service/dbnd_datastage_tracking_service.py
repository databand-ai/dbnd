# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List, Type

from dbnd_datastage_monitor.data.datastage_config_data import (
    DataStageMonitorState,
    DataStageServerConfig,
    DataStageUpdateMonitorStateRequestSchema,
)

from airflow_monitor.shared.base_server_monitor_config import TrackingServiceConfig
from airflow_monitor.shared.base_tracking_service import BaseDbndTrackingService
from dbnd._core.errors import DatabandConfigError
from dbnd._vendor.cachetools import TTLCache, cached


logger = logging.getLogger(__name__)
monitor_config_cache = TTLCache(maxsize=5, ttl=10)


class DbndDataStageTrackingService(BaseDbndTrackingService):
    def __init__(
        self,
        monitor_type: str,
        tracking_source_uid: str,
        tracking_service_config: TrackingServiceConfig,
        server_monitor_config: Type[DataStageServerConfig],
    ):
        super(DbndDataStageTrackingService, self).__init__(
            monitor_type=monitor_type,
            tracking_source_uid=tracking_source_uid,
            tracking_service_config=tracking_service_config,
            server_monitor_config=server_monitor_config,
            monitor_state_schema=DataStageUpdateMonitorStateRequestSchema,
        )

    def _fetch_source_monitor_config(self) -> List[dict]:
        result = self._api_client.api_request(
            endpoint=f"datastage_syncers/{self.tracking_source_uid}",
            method="GET",
            data=None,
        )
        return [result]

    # Cached to avoid excessive webserver calls to get config
    @cached(monitor_config_cache)
    def get_monitor_configuration(self) -> DataStageServerConfig:
        configs = self._fetch_source_monitor_config()
        if not configs:
            raise DatabandConfigError(
                f"Missing configuration for tracking source: {self.tracking_source_uid}"
            )
        return self.server_monitor_config.create(configs[0])

    def update_monitor_state(self, monitor_state):
        data, _ = self.monitor_state_schema().dump(monitor_state.as_dict())
        self._api_client.api_request(
            endpoint=f"datastage_syncers/{self.tracking_source_uid}/state",
            method="PATCH",
            data=data,
        )

    def set_running_monitor_state(self, is_monitored_server_alive: bool):
        if not is_monitored_server_alive:
            return

        self.update_monitor_state(
            DataStageMonitorState(monitor_status="Running", monitor_error_message=None)
        )

    def set_starting_monitor_state(self):
        self.update_monitor_state(
            DataStageMonitorState(
                monitor_status="Scheduled", monitor_error_message=None
            )
        )

    def get_lat_seen_date(self):
        result = self._api_client.api_request(
            endpoint=f"datastage_syncers/{self.tracking_source_uid}",
            method="GET",
            data=None,
        )
        return result["last_seen_date"]

    def update_last_seen_values(self, last_seen_date):
        self._api_client.api_request(
            endpoint=f"datastage_syncers/{self.tracking_source_uid}/last_seen_values",
            method="PATCH",
            data={"last_seen_date": last_seen_date},
        )

    def init_datastage_runs(self, datastage_runs_full_data):
        self._api_client.api_request(
            endpoint=f"tracking-monitor/{self.tracking_source_uid}/init_datastage_runs",
            method="POST",
            data=datastage_runs_full_data,
        )

    def update_datastage_runs(self, datastage_runs_full_data):
        self._api_client.api_request(
            endpoint=f"tracking-monitor/{self.tracking_source_uid}/update_datastage_runs",
            method="POST",
            data=datastage_runs_full_data,
        )

    def get_running_datastage_runs(self):
        response = self._api_client.api_request(
            endpoint=f"tracking-monitor/{self.tracking_source_uid}/active_datastage_runs",
            method="GET",
            data=None,
        )
        return response.get("datastage_runs", [])
