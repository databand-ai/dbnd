# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from typing import List, Optional, Type

from prometheus_client import generate_latest
from urllib3.exceptions import HTTPError

from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
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


if typing.TYPE_CHECKING:
    from airflow_monitor.shared.base_integration import BaseIntegration


logger = logging.getLogger(__name__)


class IntegrationManagementService:
    """
    The IntegrationManagementService class is responsible for fetching integration configurations
    from the server and building Integrations instances, and send_metrics method to send
    metrics to the server
    It takes `integration_types` for the list of supported integrations which also play
    role of integration factory
    """

    def __init__(self, integration_types: List[Type["BaseIntegration"]]):
        self._api_client: ApiClient = _get_api_client()
        self.integration_types = integration_types

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
    def get_all_integrations(
        self, monitor_config: Optional[BaseMonitorConfig] = None
    ) -> List["BaseIntegration"]:
        data = {}
        if monitor_config.syncer_name:
            data.update({"name": monitor_config.syncer_name})

        integrations = []
        for integration_type in self.integration_types:
            response = self._api_client.api_request(
                endpoint=f"integrations/config?type={integration_type.MONITOR_TYPE}",
                method="GET",
                data=data,
            )

            for config_json in response["data"]:
                integration = integration_type.build_integration(
                    config_json, monitor_config
                )
                integrations.append(integration)

        if not integrations:
            logger.warning("No integrations found")

        return integrations

    def send_metrics(self, name: str):
        metrics = generate_latest().decode("utf-8")
        if name:
            self._api_client.api_request(
                endpoint="external_prometheus_metrics",
                method="POST",
                data={"job_name": name or "monitor", "metrics": metrics},
            )
