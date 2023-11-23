# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.multiserver.airflow_integration import AirflowIntegration
from airflow_monitor.shared.adapter.adapter import ThirdPartyInfo

from .mock_airflow_adapter import MockAirflowAdapter
from .mock_airflow_data_fetcher import MockDataFetcher
from .mock_airflow_tracking_service import MockReportingService, MockTrackingService


class MockAirflowIntegration(AirflowIntegration):
    def __init__(
        self, integration_config: AirflowIntegrationConfig, mock_components_dict=None
    ):
        super().__init__(integration_config)
        self.mock_tracking_service = MockTrackingService()
        self.mock_data_fetcher = MockDataFetcher()
        self.mock_reporting_service = MockReportingService("airflow")
        self.mock_adapter = MockAirflowAdapter()
        self.mock_components_dict = mock_components_dict or {}
        self.on_integration_disabled_call_count = 0

    def get_data_fetcher(self):
        return self.mock_data_fetcher

    @property
    def reporting_service(self):
        return self.mock_reporting_service

    def get_tracking_service(self):
        return self.mock_tracking_service

    def get_components_dict(self):
        if self.mock_components_dict:
            return self.mock_components_dict

        return super(MockAirflowIntegration, self).get_components_dict()

    def get_third_party_info(self) -> ThirdPartyInfo:
        return self.mock_adapter.get_third_party_info()

    def on_integration_disabled(self):
        self.on_integration_disabled_call_count += 1
