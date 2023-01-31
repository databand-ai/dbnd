# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.shared.base_tracking_service import BaseTrackingService
from dbnd._vendor.cachetools import TTLCache


logger = logging.getLogger(__name__)
monitor_config_cache = TTLCache(maxsize=5, ttl=10)


class DbtTrackingService(BaseTrackingService):
    def __init__(self, monitor_type: str, server_id: str):
        super(DbtTrackingService, self).__init__(
            monitor_type=monitor_type, server_id=server_id
        )

    def get_last_seen_run_id(self):
        result = self._api_client.api_request(
            endpoint=f"dbt_syncers/{self.server_id}", method="GET", data=None
        )
        return result["last_seen_run_id"]

    def update_last_seen_values(self, last_seen_run_id):
        self._api_client.api_request(
            endpoint=f"dbt_syncers/{self.server_id}/last_seen_values",
            method="PATCH",
            data={"last_seen_run_id": last_seen_run_id},
        )

    def init_dbt_runs(self, dbt_runs_full_data):
        self._api_client.api_request(
            endpoint=f"tracking-monitor/{self.server_id}/save_tracking_data",
            method="POST",
            data=dbt_runs_full_data,
        )

    def update_dbt_runs(self, dbt_runs_full_data):
        self._api_client.api_request(
            endpoint=f"tracking-monitor/{self.server_id}/update_dbt_runs",
            method="POST",
            data=dbt_runs_full_data,
        )

    def get_running_dbt_run_ids(self):
        response = self._api_client.api_request(
            endpoint=f"tracking-monitor/{self.server_id}/active_dbt_runs",
            method="GET",
            data=None,
        )
        return response.get("dbt_run_ids", [])
