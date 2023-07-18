# © Copyright Databand.ai, an IBM Company 2022
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from airflow_monitor.shared.adapter.adapter import Adapter
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
    def get_components_dict(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_data_fetcher(self, server_config):
        pass

    @abstractmethod
    def get_integration_management_service(self) -> BaseTrackingService:
        pass

    @abstractmethod
    def get_tracking_service(self, server_config: BaseServerConfig):
        pass

    def get_adapter(self, server_config: BaseServerConfig) -> Optional[Adapter]:
        return None

    def get_components(
        self,
        integration_config: BaseServerConfig,
        integration_management_service: IntegrationManagementService,
    ):
        tracking_service = self.get_tracking_service(integration_config)
        data_fetcher = self.get_data_fetcher(integration_config)
        components_dict = self.get_components_dict()
        all_components = []
        for _, syncer_class in components_dict.items():
            syncer_instance = syncer_class(
                config=integration_config,
                tracking_service=tracking_service,
                integration_management_service=integration_management_service,
                data_fetcher=data_fetcher,
            )
            all_components.append(syncer_instance)

        return all_components

    def on_integration_disabled(
        self,
        integration_config: BaseServerConfig,
        integration_management_service: IntegrationManagementService,
    ):
        pass
