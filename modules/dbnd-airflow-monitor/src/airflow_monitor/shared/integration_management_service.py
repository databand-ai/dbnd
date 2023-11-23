# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict, List, Optional

from prometheus_client import generate_latest
from urllib3.exceptions import HTTPError

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
    The IntegrationManagementService class is responsible for fetching integration configurations
    from the server, and provides `send_metrics` method to send metrics to the server
    """

    def __init__(self):
        self._api_client: ApiClient = _get_api_client()

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
    def get_all_integration_configs(
        self, monitor_type: str, syncer_name: Optional[str] = None
    ) -> List[Dict]:
        data = {}
        if syncer_name:
            data.update({"name": syncer_name})

        response = self._api_client.api_request(
            endpoint=f"integrations/config?type={monitor_type}", method="GET", data=data
        )
        integration_configs = response["data"]

        if not integration_configs:
            logger.warning("No integrations found")

        return integration_configs

    def send_metrics(self, name: str):
        metrics = generate_latest().decode("utf-8")
        if name:
            self._api_client.api_request(
                endpoint="external_prometheus_metrics",
                method="POST",
                data={"job_name": name or "monitor", "metrics": metrics},
            )
