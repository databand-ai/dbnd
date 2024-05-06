# © Copyright Databand.ai, an IBM Company 2022
from abc import ABC, abstractmethod
from typing import ClassVar, List, Optional, Type

import airflow_monitor

from airflow_monitor.shared.adapter.adapter import ThirdPartyInfo
from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.shared.base_integration_config import BaseIntegrationConfig
from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig
from airflow_monitor.shared.reporting_service import ReportingService


class BaseIntegration(ABC):
    """
    BaseIntegration is an abstract base class for integrating with a third-party system.

    It provides a standardized way to integrate with a third-party system,
    including reporting errors and metadata to the Integration backend.

    Subclasses should implement the following methods:

    * `get_third_party_info`: Returns information about the third-party system,
      such as a list of errors and version information.
    * `get_components`: Returns a list of components associated with the integration.
    * `on_integration_disabled`: Called when the integration is disabled.
    * `on_integration_enabled`: Called when the integration is enabled.

    Subclasses should also set the `MONITOR_TYPE` class variable to the type of integration.
    """

    MONITOR_TYPE: ClassVar[str]

    # two bellow should match:
    #  * CONFIG_CLASS to actually create config
    #  * integration_config typing for better intellisense
    CONFIG_CLASS: ClassVar[Type[BaseIntegrationConfig]] = BaseIntegrationConfig
    config: BaseIntegrationConfig

    def __init__(self, integration_config: BaseIntegrationConfig) -> None:
        self.config = integration_config

    @classmethod
    def build_integration(
        cls, config_json: dict, monitor_config: Optional[BaseMonitorConfig]
    ) -> "BaseIntegration":
        integration_config = cls.CONFIG_CLASS.create(config_json, monitor_config)
        return cls(integration_config)

    @property
    def reporting_service(self) -> ReportingService:
        return ReportingService(monitor_type=self.MONITOR_TYPE)

    def get_third_party_info(self) -> Optional[ThirdPartyInfo]:
        return None

    @abstractmethod
    def get_components(self) -> List[BaseComponent]:
        raise NotImplementedError

    def on_integration_disabled(self) -> None:
        pass

    def on_integration_enabled(self) -> None:
        self.reporting_service.clean_error_message(self.config.uid)
        self._report_third_party_data()

    def _report_third_party_data(self) -> None:
        # This is the version of the monitor, since currently the shared logic exists
        # in airflow_monitor package we import this package and get the version
        metadata = {"monitor_version": airflow_monitor.__version__}

        # pylint: disable=assignment-from-none
        third_party_info = self.get_third_party_info()
        if third_party_info and third_party_info.error_list:
            formatted_error_list = ", ".join(third_party_info.error_list)
            self.reporting_service.report_error(
                self.config.uid,
                f"verify_environment_{self.config.uid}",
                formatted_error_list,
            )

        if third_party_info and third_party_info.metadata:
            metadata.update(third_party_info.metadata)

        self.reporting_service.report_metadata(self.config.uid, metadata)
