# © Copyright Databand.ai, an IBM Company 2022
import logging

from typing import List

from airflow_monitor.common.metric_reporter import METRIC_REPORTER, measure_time
from airflow_monitor.shared.adapter.adapter import AssetState, AssetToState
from airflow_monitor.shared.utils import _get_api_client
from dbnd._vendor.cachetools import TTLCache
from dbnd._vendor.tenacity import retry, stop_after_attempt


logger = logging.getLogger(__name__)

monitor_config_cache = TTLCache(maxsize=5, ttl=10)
LONG_REQUEST_TIMEOUT = 300


class BaseTrackingService:
    def __init__(self, monitor_type: str, tracking_source_uid: str):
        self.monitor_type = monitor_type
        self.tracking_source_uid = tracking_source_uid
        self._api_client = _get_api_client()

    @measure_time(metric=METRIC_REPORTER.exporter_response_time, label=__file__)
    @retry(stop=stop_after_attempt(2), reraise=True)
    def save_tracking_data(self, assets_data: dict):
        boxed_payload = {"metadata": {"format": self.monitor_type}, "data": assets_data}
        return self._api_client.api_request(
            endpoint=f"tracking-monitor/{self.tracking_source_uid}/save_tracking_data",
            method="POST",
            data=boxed_payload,
            request_timeout=LONG_REQUEST_TIMEOUT,
        )

    @retry(stop=stop_after_attempt(2), reraise=True)
    def save_assets_state(
        self,
        integration_id: str,
        syncer_instance_id: str,
        assets_to_state: List[AssetToState],
    ):
        data_to_send = [asset_to_state.asdict() for asset_to_state in assets_to_state]
        self._api_client.api_request(
            endpoint=f"tracking-monitor/{integration_id}/assets/run?syncer_instance_id={syncer_instance_id}",
            method="PUT",
            data=data_to_send,
        )

    @retry(stop=stop_after_attempt(2), reraise=True)
    def get_active_assets(
        self, integration_id: str, syncer_instance_id: str
    ) -> List[AssetToState]:
        result = self._api_client.api_request(
            endpoint=f"tracking-monitor/{integration_id}/assets/run?states={','.join(AssetState.get_active_states())}&syncer_instance_id={syncer_instance_id}",
            method="GET",
            data=None,
        )
        assets_to_state = []
        for asset_to_state_dict in result:
            try:
                assets_to_state.append(AssetToState.from_dict(asset_to_state_dict))
            except Exception:
                logger.exception("failed to parse asset data, asset will be skipped")
                continue
        return assets_to_state

    @retry(stop=stop_after_attempt(2), reraise=True)
    def update_last_cursor(
        self, integration_id: str, syncer_instance_id: str, state: str, data: str
    ):
        self._api_client.api_request(
            endpoint=f"tracking-monitor/{integration_id}/assets/state/cursor?syncer_instance_id={syncer_instance_id}",
            method="PUT",
            data={"state": state, "data": {"last_cursor_value": data}},
        )

    @retry(stop=stop_after_attempt(2), reraise=True)
    def get_last_cursor(self, integration_id: str, syncer_instance_id: str):
        result = self._api_client.api_request(
            endpoint=f"tracking-monitor/{integration_id}/assets/state/cursor?syncer_instance_id={syncer_instance_id}",
            method="GET",
            data=None,
        )
        return result.get("data", {}).get("last_cursor_value")
