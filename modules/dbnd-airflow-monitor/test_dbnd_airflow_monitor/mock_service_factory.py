# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.shared.monitor_services_factory import MonitorServicesFactory

from .mock_airflow_data_fetcher import MockDataFetcher
from .mock_airflow_tracking_service import MockServersConfigService, MockTrackingService


class MockAirflowServicesFactory(MonitorServicesFactory):
    def __init__(self):
        self.mock_servers_config_service = MockServersConfigService()
        self.mock_tracking_service = MockTrackingService()
        self.mock_data_fetcher = MockDataFetcher()

    def get_data_fetcher(self, server_config):
        return self.mock_data_fetcher

    def get_servers_configuration_service(self):
        return self.mock_servers_config_service

    def get_tracking_service(self, tracking_source_uid):
        return self.mock_tracking_service
