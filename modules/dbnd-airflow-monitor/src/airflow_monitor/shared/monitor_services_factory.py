# © Copyright Databand.ai, an IBM Company 2022
from abc import ABC, abstractmethod
from typing import Optional

from airflow_monitor.shared.adapter.adapter import ThirdPartyInfo
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_tracking_service import BaseTrackingService
from airflow_monitor.shared.integration_management_service import (
    IntegrationManagementService,
)


class MonitorServicesFactory(ABC):
    """
    This class is a factory that returns all the required components for running a monitor
    Inherit from this class, implement the methods and pass it to a MultiServerMonitor instance.
    """

    @abstractmethod
    def get_integration_management_service(self) -> BaseTrackingService:
        pass

    def get_third_party_info(
        self, server_config: BaseServerConfig
    ) -> Optional[ThirdPartyInfo]:
        return None

    def get_components(
        self,
        integration_config: BaseServerConfig,
        integration_management_service: IntegrationManagementService,
    ):
        raise NotImplementedError

    def on_integration_disabled(
        self,
        integration_config: BaseServerConfig,
        integration_management_service: IntegrationManagementService,
    ):
        pass
