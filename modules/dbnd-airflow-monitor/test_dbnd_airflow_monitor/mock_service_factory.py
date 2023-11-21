# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.multiserver.airflow_services_factory import AirflowServicesFactory
from airflow_monitor.shared.adapter.adapter import ThirdPartyInfo
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig

from .mock_airflow_adapter import MockAirflowAdapter
from .mock_airflow_data_fetcher import MockDataFetcher
from .mock_airflow_tracking_service import (
    MockIntegrationManagementService,
    MockReportingService,
    MockTrackingService,
)


class MockAirflowServicesFactory(AirflowServicesFactory):
    def __init__(self):
        self.mock_tracking_service = MockTrackingService()
        self.mock_data_fetcher = MockDataFetcher()
        self.mock_integration_management_service = MockIntegrationManagementService(
            "airflow", AirflowServerConfig
        )
        self._reporting_service = MockReportingService("airflow")
        self.mock_adapter = MockAirflowAdapter()
        self.mock_components_dict = {}
        self.on_integration_disabled_call_count = 0

    def get_data_fetcher(self, server_config):
        return self.mock_data_fetcher

    def get_integration_management_service(self):
        return self.mock_integration_management_service

    @property
    def reporting_service(self):
        return self._reporting_service

    def get_tracking_service(self, server_config):
        return self.mock_tracking_service

    def get_components_dict(self):
        if self.mock_components_dict:
            return self.mock_components_dict

        return super(MockAirflowServicesFactory, self).get_components_dict()

    def get_third_party_info(self, server_config: BaseServerConfig) -> ThirdPartyInfo:
        return self.mock_adapter.get_third_party_info()

    def on_integration_disabled(self, integration_config: BaseServerConfig):
        self.on_integration_disabled_call_count += 1
