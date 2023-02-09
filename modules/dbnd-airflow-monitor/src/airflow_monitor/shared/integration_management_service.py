# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict, List, Optional, Type
from uuid import UUID

from prometheus_client import generate_latest
from urllib3.exceptions import HTTPError

import dbnd

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.error_aggregator import ErrorAggregator
from airflow_monitor.shared.utils import _get_api_client
from dbnd._core.errors.base import DatabandConnectionException
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor.tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_delay,
    wait_exponential,
)
from dbnd.utils.api_client import ApiClient


logger = logging.getLogger(__name__)


class IntegrationManagementService:
    """
    This class is responsible for all communication with configured web server's syncer including
    fetching configuration, updating state (starting/running), Prometheus metrics and error sending
    """

    def __init__(
        self,
        monitor_type: str,
        server_monitor_config: Type[BaseServerConfig],
        integrations_name_filter: Optional[Dict[str, str]] = None,
    ):
        self.monitor_type: str = monitor_type
        self._api_client: ApiClient = _get_api_client()
        self.server_monitor_config = server_monitor_config

        self._error_aggregator = ErrorAggregator()
        self._integrations_name_filter = integrations_name_filter

    @retry(
        stop=stop_after_delay(30),
        retry=(
            retry_if_exception_type(HTTPError)
            | retry_if_exception_type(DatabandConnectionException)
        ),
        wait=wait_exponential(multiplier=1, max=10),
        before_sleep=before_sleep_log(logger, logging.DEBUG),
        reraise=True,
    )
    def get_all_servers_configuration(
        self, monitor_config: Optional[BaseMonitorConfig] = None
    ) -> List[BaseServerConfig]:
        data = {}
        if self._integrations_name_filter:
            data.update({"name": self._integrations_name_filter})

        response = self._api_client.api_request(
            endpoint=f"integrations/config?type={self.monitor_type}",
            method="GET",
            data=data,
        )
        result_json = response["data"]

        servers_configs = [
            self.server_monitor_config.create(server, monitor_config)
            for server in result_json
        ]

        if not servers_configs:
            logger.warning("No integrations found")
            return servers_configs

        return servers_configs

    def report_monitor_time_data(
        self, integration_uid: UUID, synced_new_data: bool = False
    ):
        current_time = utcnow().isoformat()
        data = {"last_sync_time": current_time}
        if synced_new_data:
            data["last_update_time"] = current_time
        self._api_client.api_request(
            endpoint=f"integrations/{integration_uid}/monitor_time_data?type={self.monitor_type}",
            method="PATCH",
            data=data,
        )

    def report_metadata(self, integration_uid: UUID, metadata: dict):
        self._api_client.api_request(
            endpoint=f"integrations/{integration_uid}/metadata?type={self.monitor_type}",
            method="PATCH",
            data={"monitor_metadata": metadata},
        )

    def report_error(
        self, integration_uid: UUID, full_function_name: str, err_message: str
    ):
        res = self._error_aggregator.report(full_function_name, err_message)
        if res.should_update:
            self._api_client.api_request(
                endpoint=f"integrations/{integration_uid}/error?type={self.monitor_type}",
                method="PATCH",
                data={"monitor_error_message": res.message},
            )

    def send_metrics(self, name: str):
        metrics = generate_latest().decode("utf-8")
        if name:
            self._api_client.api_request(
                endpoint="external_prometheus_metrics",
                method="POST",
                data={
                    "job_name": name or f"{self.monitor_type}-monitor",
                    "metrics": metrics,
                },
            )

    def report_exception_to_web_server(self, exception: str):
        data = {
            "dbnd_version": dbnd.__version__,
            "source": f"{self.monitor_type}_monitor",
            "stack_trace": exception,
            "timestamp": utcnow().isoformat(),
        }
        return self._api_client.api_request(
            endpoint="log_exception", method="POST", data=data
        )
