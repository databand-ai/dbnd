# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List, Optional, Type

from urllib3.exceptions import HTTPError

import dbnd

from airflow_monitor.shared.base_monitor_config import (
    BaseMonitorConfig,
    BaseMonitorState,
)
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


class BaseSyncerManagementService:
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

        self._error_aggregator = ErrorAggregator()

    def _get_monitor_config_data(self):
        response = self._api_client.api_request(
            endpoint=f"source_monitor/{self.monitor_type}/config",
            method="GET",
            data=None,
        )
        result_json = response["data"]
        return result_json

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
        result_json = self._get_monitor_config_data()
        servers_configs = [
            self.server_monitor_config.create(server, monitor_config)
            for server in result_json
        ]

        if not servers_configs:
            logger.warning("No servers found")
            return servers_configs

        active_servers = [s for s in servers_configs if s.is_sync_enabled]

        if not active_servers:
            logger.warning("No enabled servers found")

        return active_servers

    def send_prometheus_metrics(self, full_metrics: str, job_name: Optional[str]):
        self._api_client.api_request(
            endpoint="tracking-monitor/save_monitor_metrics",
            method="POST",
            data={
                "job_name": job_name or f"{self.monitor_type}-monitor",
                "metrics": full_metrics,
            },
        )

    def send_metrics(self, monitor_config):
        pass

    def update_monitor_state(self, server_id, monitor_state: BaseMonitorState):
        pass

    def set_running_monitor_state(self, server_id):
        pass

    def set_starting_monitor_state(self, server_id):
        pass

    def update_last_sync_time(self, server_id):
        pass

    def report_syncer_error(self, server_id, full_function_name, err_message):
        res = self._error_aggregator.report(full_function_name, err_message)
        if res.should_update:
            self.update_monitor_state(
                server_id, BaseMonitorState(monitor_error_message=res.message)
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
