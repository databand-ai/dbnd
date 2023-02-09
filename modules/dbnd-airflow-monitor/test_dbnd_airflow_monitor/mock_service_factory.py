# © Copyright Databand.ai, an IBM Company 2022
from unittest.mock import MagicMock

from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.multiserver.airflow_services_factory import AirflowServicesFactory

from .mock_airflow_adapter import MockAirflowAdapter
from .mock_airflow_data_fetcher import MockDataFetcher
from .mock_airflow_tracking_service import (
    MockIntegrationManagementService,
    MockTrackingService,
)


class MockAirflowServicesFactory(AirflowServicesFactory):
    def __init__(self):
        self.mock_tracking_service = MockTrackingService()
        self.mock_data_fetcher = MockDataFetcher()
        self.mock_integration_management_service = MockIntegrationManagementService(
            "airflow", AirflowServerConfig
        )
        self.mock_adapter = MockAirflowAdapter(MagicMock)
        self.mock_components_dict = {}

    def get_data_fetcher(self, server_config):
        return self.mock_data_fetcher

    def get_integration_management_service(self):
        return self.mock_integration_management_service

    def get_tracking_service(self, server_config):
        return self.mock_tracking_service

    def get_components_dict(self):
        if self.mock_components_dict:
            return self.mock_components_dict

        return super(MockAirflowServicesFactory, self).get_components_dict()

    def get_adapter(self, server_config):
        return self.mock_adapter
