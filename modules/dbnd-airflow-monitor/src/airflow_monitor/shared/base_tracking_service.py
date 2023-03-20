# © Copyright Databand.ai, an IBM Company 2022
import logging

from airflow_monitor.shared.adapter.adapter import AssetState, AssetToState
from airflow_monitor.shared.utils import _get_api_client
from dbnd._vendor.cachetools import TTLCache


logger = logging.getLogger(__name__)
monitor_config_cache = TTLCache(maxsize=5, ttl=10)


class BaseTrackingService:
    def __init__(self, monitor_type: str, server_id: str, integration_id: str = None):
        self.monitor_type = monitor_type
        self.server_id = server_id
        self._api_client = _get_api_client()

    def _generate_url_for_tracking_service(self, name: str) -> str:
        return f"tracking-monitor/{self.server_id}/{name}"

    def _make_request(
        self,
        name: str,
        method: str,
        data: dict,
        query: dict = None,
        request_timeout: int = None,
    ) -> dict:
        return self._api_client.api_request(
            endpoint=self._generate_url_for_tracking_service(name),
            method=method,
            data=data,
            query=query,
            request_timeout=request_timeout,
        )

    def save_tracking_data(self, assets_data: dict):
        self._api_client.api_request(
            endpoint=f"tracking-monitor/{self.server_id}/save_tracking_data",
            method="POST",
            data=assets_data,
        )

    def save_assets_state(
        self,
        integration_id: str,
        syncer_instance_id: str,
        assets_to_state: list[AssetToState],
    ):
        data_to_send = [asset_to_state.asdict() for asset_to_state in assets_to_state]
        self._api_client.api_request(
            endpoint=f"tracking-monitor/{integration_id}/assets/run?syncer_instance_id={syncer_instance_id}",
            method="PUT",
            data=data_to_send,
        )

    def get_active_assets(
        self, integration_id: str, syncer_instance_id: str
    ) -> list[AssetToState]:
        result = self._api_client.api_request(
            endpoint=f"tracking-monitor/{integration_id}/assets/run?states={','.join(AssetState.get_active_states())}&syncer_instance_id={syncer_instance_id}",
            method="GET",
            data=None,
        )
        assets_to_state = []
        for asset_to_state_dict in result:
            try:
                assets_to_state.append(AssetToState.from_dict(asset_to_state_dict))
            except:
                logger.exception("failed to parse asset data, asset will be skipped")
                continue
        return assets_to_state

    def update_last_cursor(
        self, integration_id: str, syncer_instance_id: str, state: str, data: str
    ):
        self._api_client.api_request(
            endpoint=f"tracking-monitor/{integration_id}/assets/state/cursor?syncer_instance_id={syncer_instance_id}",
            method="PUT",
            data={"state": state, "data": {"last_cursor_value": data}},
        )

    def get_last_cursor(self, integration_id: str, syncer_instance_id: str):
        result = self._api_client.api_request(
            endpoint=f"tracking-monitor/{integration_id}/assets/state/cursor?syncer_instance_id={syncer_instance_id}",
            method="GET",
            data=None,
        )
        return result.get("data", {}).get("last_cursor_value")
