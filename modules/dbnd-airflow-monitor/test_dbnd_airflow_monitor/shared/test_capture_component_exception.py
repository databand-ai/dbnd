# © Copyright Databand.ai, an IBM Company 2022
from unittest.mock import MagicMock

import pytest

from mock import patch

from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.shared.errors import ClientConnectionError
from airflow_monitor.syncer.runtime_syncer import AirflowRuntimeSyncer
from dbnd._core.errors import DatabandError
from dbnd._core.utils.uid_utils import get_uuid


class TestCaptureComponentException:
    @pytest.fixture
    def runtime_syncer(
        self, mock_data_fetcher, mock_tracking_service, mock_reporting_service
    ):
        syncer = AirflowRuntimeSyncer(
            config=AirflowIntegrationConfig(
                uid=get_uuid(),
                source_name="test",
                source_type="airflow",
                tracking_source_uid=mock_tracking_service.tracking_source_uid,
            ),
            tracking_service=mock_tracking_service,
            data_fetcher=mock_data_fetcher,
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

    @patch("airflow_monitor.shared.error_handler._log_exception_to_server")
    def test_calling__log_exception_to_server(
        self, mock__log_exception_to_server, runtime_syncer
    ):
        runtime_syncer._sync_once = MagicMock()
        runtime_syncer._sync_once.side_effect = DatabandError()
        try:
            runtime_syncer.sync_once()
        except Exception:
            pass

        mock__log_exception_to_server.assert_called_once()

    @patch("airflow_monitor.shared.error_handler._log_exception_to_server")
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
