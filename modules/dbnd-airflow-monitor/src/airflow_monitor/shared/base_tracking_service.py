# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.shared.utils import _get_api_client
from dbnd._vendor.cachetools import TTLCache


logger = logging.getLogger(__name__)
monitor_config_cache = TTLCache(maxsize=5, ttl=10)


class BaseTrackingService:
    def __init__(self, monitor_type: str, server_id: str):
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
