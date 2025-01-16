# © Copyright Databand.ai, an IBM Company 2022
from unittest.mock import MagicMock

import pytest

from mock import patch

from dbnd._core.errors import DatabandError
from dbnd._core.utils.uid_utils import get_uuid
from dbnd_monitor.base_component import BaseComponent
from dbnd_monitor.base_integration_config import BaseIntegrationConfig
from dbnd_monitor.error_aggregator import ComponentErrorAggregator, ErrorAggregator
from dbnd_monitor.errors import ClientConnectionError
from dbnd_monitor.reporting_service import ReportingService


class MockSyncer(BaseComponent):
    SYNCER_TYPE = "syncer"


@pytest.fixture(params=[ErrorAggregator, ComponentErrorAggregator, None])
def mock_reporting_service(request):
    with patch("dbnd.utils.api_client.ApiClient.api_request"):
        aggregator = request.param
        if aggregator:
            yield ReportingService("airflow", aggregator=aggregator)
        else:
            yield ReportingService("airflow")


class TestCaptureComponentException:
    @pytest.fixture
    def runtime_syncer(self, mock_tracking_service, mock_reporting_service):
        syncer = MockSyncer(
            config=BaseIntegrationConfig(
                uid=get_uuid(),
                source_name="test",
                source_type="airflow",
                tracking_source_uid=mock_tracking_service.tracking_source_uid,
            ),
            tracking_service=mock_tracking_service,
            reporting_service=mock_reporting_service,
        )

        with patch.object(
            syncer, "tracking_service", wraps=syncer.tracking_service
        ), patch.object(
            syncer, "data_fetcher", wraps=syncer.data_fetcher
        ), patch.object(
            syncer, "reporting_service", wraps=syncer.reporting_service
        ):
            yield syncer

    @patch("dbnd_monitor.error_handler._log_exception_to_server")
    def test_calling__log_exception_to_server(
        self, mock__log_exception_to_server, runtime_syncer
    ):
        runtime_syncer._sync_once = MagicMock()
        runtime_syncer._sync_once.side_effect = DatabandError()
        try:
            runtime_syncer.sync_once()
        except Exception as e:
            print(e)

        mock__log_exception_to_server.assert_called_once()

    @patch("dbnd_monitor.error_handler._log_exception_to_server")
    def test_not_calling_log_exception_to_server(
        self, mock__log_exception_to_server, runtime_syncer
    ):
        runtime_syncer._sync_once = MagicMock()
        runtime_syncer._sync_once.side_effect = ClientConnectionError()
        try:
            runtime_syncer.sync_once()
        except Exception:
            pass

        mock__log_exception_to_server.assert_not_called()
