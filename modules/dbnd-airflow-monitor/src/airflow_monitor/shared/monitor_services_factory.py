# © Copyright Databand.ai, an IBM Company 2022
from abc import ABC, abstractmethod
from typing import Optional

import airflow_monitor

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

    def on_integration_enabled(
        self,
        integration_config: BaseServerConfig,
        integration_management_service: IntegrationManagementService,
    ) -> None:
        integration_management_service.clean_error_message(integration_config.uid)
        self._report_third_party_data(
            integration_config, integration_management_service
        )

    def _report_third_party_data(
        self,
        integration_config: BaseServerConfig,
        integration_management_service: IntegrationManagementService,
    ) -> None:
        # This is the version of the monitor, since currently the shared logic exists
        # in airflow_monitor package we import this package and get the version
        metadata = {"monitor_version": airflow_monitor.__version__}

        # pylint: disable=assignment-from-none
        third_party_info = self.get_third_party_info(integration_config)
        if third_party_info and third_party_info.error_list:
            formatted_error_list = ", ".join(third_party_info.error_list)
            integration_management_service.report_error(
                integration_config.uid,
                f"verify_environment_{integration_config.uid}",
                formatted_error_list,
            )

        if third_party_info and third_party_info.metadata:
            metadata.update(third_party_info.metadata)

        integration_management_service.report_metadata(integration_config.uid, metadata)
