import logging

from typing import List, Optional, Type

import dbnd

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import (
    BaseServerConfig,
    TrackingServiceConfig,
)
from airflow_monitor.shared.error_aggregator import ErrorAggregator
from dbnd._core.errors import DatabandConfigError
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor.cachetools import TTLCache, cached
from dbnd.api.serialization.tracking import (
    BaseSourceMonitorState,
    BaseSourceMonitorStateSchema,
)
from dbnd.utils.api_client import ApiClient


DEFAULT_REQUEST_TIMEOUT = 30  # Seconds

logger = logging.getLogger(__name__)
monitor_config_cache = TTLCache(maxsize=5, ttl=10)


def _get_api_client(tracking_service_config: TrackingServiceConfig) -> ApiClient:
    if tracking_service_config.access_token:
        credentials = {"token": tracking_service_config.access_token}
    else:
        credentials = {
            "username": tracking_service_config.user,
            "password": tracking_service_config.password,
        }
    return ApiClient(
        tracking_service_config.url,
        credentials=credentials,
        default_request_timeout=DEFAULT_REQUEST_TIMEOUT,
    )


class BaseDbndTrackingService(object):
    def __init__(
        self,
        monitor_type: str,
        tracking_source_uid: str,
        tracking_service_config: TrackingServiceConfig,
        server_monitor_config: Type[BaseServerConfig],
        monitor_state_schema: Type[BaseSourceMonitorStateSchema],
    ):
        self.monitor_type = monitor_type
        self.tracking_source_uid = tracking_source_uid
        self.server_monitor_config = server_monitor_config
        self.monitor_state_schema = monitor_state_schema
        self._api_client = _get_api_client(tracking_service_config)

        self._error_aggregator = ErrorAggregator()

    def _generate_url_for_tracking_service(self, name: str) -> str:
        return f"tracking-monitor/{self.tracking_source_uid}/{name}"

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

    def _fetch_source_monitor_config(self) -> List[dict]:
        response = self._api_client.api_request(
            endpoint=f"source_monitor/{self.monitor_type}/{self.tracking_source_uid}/config",
            method="GET",
            data=None,
        )
        configs = response.get("data")
        return configs

    # Cached to avoid excessive webserver calles to get config
    @cached(monitor_config_cache)
    def get_monitor_configuration(self) -> BaseServerConfig:
        configs = self._fetch_source_monitor_config()
        if not configs:
            raise DatabandConfigError(
                f"Missing configuration for tracking source: {self.tracking_source_uid}"
            )
        return self.server_monitor_config.create(configs[0])

    def update_monitor_state(self, monitor_state):
        data, _ = self.monitor_state_schema().dump(monitor_state.as_dict())

        self._api_client.api_request(
            endpoint=f"source_monitor/{self.monitor_type}/{self.tracking_source_uid}/state",
            method="POST",
            data=data,
        )

    def report_error(self, reporting_obj_ref, err_message):
        res = self._error_aggregator.report(reporting_obj_ref, err_message)
        if res.should_update:
            self.update_monitor_state(
                BaseSourceMonitorState(monitor_error_message=res.message)
            )

    def report_exception(self, exception: str):
        data = {
            "dbnd_version": dbnd.__version__,
            "source": f"{self.monitor_type}_monitor",
            "stack_trace": exception,
            "timestamp": utcnow().isoformat(),
        }
        return self._api_client.api_request(
            endpoint="log_exception", method="POST", data=data
        )


class WebServersConfigurationService(object):
    def __init__(
        self,
        monitor_type: str,
        tracking_service_config: TrackingServiceConfig,
        server_monitor_config: Type[BaseServerConfig],
    ):
        self.monitor_type: str = monitor_type  # airflow_monitor / datasource_monitor
        self._api_client: ApiClient = _get_api_client(tracking_service_config)
        self.server_monitor_config = server_monitor_config

    def _get_monitor_config_data(self):
        response = self._api_client.api_request(
            endpoint=f"source_monitor/{self.monitor_type}/config",
            method="GET",
            data=None,
        )
        result_json = response["data"]
        return result_json

    def get_all_servers_configuration(
        self, monitor_config: Optional[BaseMonitorConfig] = None
    ) -> List[BaseServerConfig]:
        result_json = self._get_monitor_config_data()
        servers_configs = [
            self.server_monitor_config.create(server, monitor_config)
            for server in result_json
        ]
        return servers_configs

    def send_prometheus_metrics(self, full_metrics: str, job_name: Optional[str]):
        self._api_client.api_request(
            endpoint="tracking-monitor/save_monitor_metrics",
            method="POST",
            data={
                "job_name": job_name or f"{self.monitor_type}-monitor",
                "metrics": full_metrics,
            },
        )

    def report_exception(self, exception: str):
        data = {
            "dbnd_version": dbnd.__version__,
            "source": f"{self.monitor_type}_monitor",
            "stack_trace": exception,
            "timestamp": utcnow().isoformat(),
        }
        return self._api_client.api_request(
            endpoint="log_exception", method="POST", data=data
        )
