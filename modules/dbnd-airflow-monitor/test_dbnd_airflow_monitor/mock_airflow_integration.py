# © Copyright Databand.ai, an IBM Company 2022
from unittest.mock import patch

from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.multiserver.airflow_integration import AirflowIntegration
from dbnd_monitor.reporting_service import ReportingService

from .mock_airflow_adapter import MockAirflowAdapter
from .mock_airflow_data_fetcher import MockDataFetcher
from .mock_airflow_tracking_service import MockReportingService, MockTrackingService


class MockAirflowIntegration(AirflowIntegration):
    def __init__(self, integration_config: AirflowIntegrationConfig):
        self.mock_data_fetcher = MockDataFetcher()
        self.mock_reporting_service = MockReportingService("airflow")
        self.mock_adapter = MockAirflowAdapter(
            integration_config, self.mock_data_fetcher
        )
        self.on_integration_disabled_call_count = 0

        with patch(
            "airflow_monitor.multiserver.airflow_integration.AirflowAdapter",
            return_value=self.mock_adapter,
        ), patch(
            "airflow_monitor.multiserver.airflow_integration.AirflowTrackingService",
            return_value=MockTrackingService(),
        ):
            super().__init__(integration_config)

    @property
    def reporting_service(self) -> ReportingService:
        return self.mock_reporting_service

    def on_integration_disabled(self):
        self.on_integration_disabled_call_count += 1
