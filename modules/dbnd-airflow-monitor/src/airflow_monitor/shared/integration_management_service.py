# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List, Optional, Type

from prometheus_client import generate_latest
from urllib3.exceptions import HTTPError

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.utils import _get_api_client
from dbnd._core.errors.base import DatabandConnectionException
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
        self, monitor_type: str, server_monitor_config: Type[BaseServerConfig]
    ):
        self.monitor_type: str = monitor_type
        self._api_client: ApiClient = _get_api_client()
        self.server_monitor_config = server_monitor_config

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
        if monitor_config.syncer_name:
            data.update({"name": monitor_config.syncer_name})

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
