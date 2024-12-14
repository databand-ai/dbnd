# © Copyright Databand.ai, an IBM Company 2022

import uuid

import pytest

from mock import MagicMock, patch

import airflow_monitor

from airflow_monitor.common.airflow_data import PluginMetadata
from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.config import AirflowMonitorConfig
from dbnd._core.utils.dotdict import _as_dotted_dict
from dbnd._core.utils.uid_utils import get_uuid
from dbnd_monitor.base_component import BaseComponent
from dbnd_monitor.multiserver import MultiServerMonitor
from test_dbnd_airflow_monitor.airflow_utils import TestConnectionError
from test_dbnd_airflow_monitor.mock_airflow_integration import MockAirflowIntegration


MOCK_SERVER_1_CONFIG = {
    "uid": get_uuid(),
    "source_type": "airflow",
    "source_name": "mock_server_1",
    "tracking_source_uid": uuid.uuid4(),
    "sync_interval": 0,  # Important so that the same syncer can run for another iteration
}
MOCK_SERVER_2_CONFIG = {
    "uid": get_uuid(),
    "source_type": "airflow",
    "source_name": "mock_server_2",
    "tracking_source_uid": uuid.uuid4(),
}
MOCK_SERVER_3_CONFIG = {
    "uid": get_uuid(),
    "source_type": "airflow",
    "source_name": "mock_server_3",
    "tracking_source_uid": uuid.uuid4(),
}
MOCK_SERVER_4_CONFIG = {
    "uid": get_uuid(),
    "source_type": "airflow",
    "source_name": "mock_server_4",
    "tracking_source_uid": uuid.uuid4(),
    "sync_interval": 0,  # Important so that the same syncer can run for another iteration
}


@pytest.fixture
def airflow_monitor_config():
    # use dummy local_dag_folder to generate "new" config object
    # (that won't conflict with monitor v1 global configuration)
    return AirflowMonitorConfig(local_dag_folder="/tmp")


@pytest.fixture
def multi_server(mock_integration_management_service, airflow_monitor_config):
    yield MultiServerMonitor(
        monitor_config=airflow_monitor_config,
        integration_management_service=mock_integration_management_service,
        integration_types=[MockAirflowIntegration],
    )


@pytest.fixture
def multi_server_with_multiple_integration_types(
    mock_integration_management_service, airflow_monitor_config
):
    yield MultiServerMonitor(
        monitor_config=airflow_monitor_config,
        integration_management_service=mock_integration_management_service,
        integration_types=[MockAirflowIntegration, MockAirflowIntegration],
    )


def count_logged_exceptions(caplog):
    logged_exceptions = [record for record in caplog.records if record.exc_info]
    return len(logged_exceptions)


class MockSyncer(BaseComponent):
    SYNCER_TYPE = "mock_syncer"

    def __init__(self, *args, **kwargs):
        super(MockSyncer, self).__init__(*args, **kwargs)
        self.sync_count = 0
        self.should_fail = False

    def _sync_once(self):
        if self.should_fail:
            raise Exception("Mock - should fail")
        self.sync_count += 1

    def emulate_start_syncer(self, *args, **kwargs):
        return self


class TestMultiServer(object):
    def test_01_no_servers(self, multi_server):
        multi_server.run_once()

        # no servers - should stay empty
        assert not multi_server.active_integrations

    def test_02_config_service_not_available(
        self, multi_server, mock_integration_management_service
    ):
        mock_integration_management_service.alive = False
        with pytest.raises(TestConnectionError):
            multi_server.run_once()

    def test_03_empty_config(self, multi_server, caplog):
        # server config is empty (all components disabled) - nothing should run
        multi_server.get_integrations = MagicMock(
            return_value=[
                MockAirflowIntegration(AirflowIntegrationConfig(**config))
                for config in (MOCK_SERVER_1_CONFIG, MOCK_SERVER_2_CONFIG)
            ]
        )
        multi_server.run_once()

        assert len(multi_server.active_integrations) == 2
        for components_dict in multi_server.active_integrations.values():
            # All components should be there regardless if they are disabled in the config (no longer supported feature)
            assert len(components_dict) == 2
        assert not count_logged_exceptions(caplog)

    def test_04_single_server_single_component(self, multi_server, caplog):
        components = {"state_sync": MockSyncer}
        mock_airflow_integration = MockAirflowIntegration(
            AirflowIntegrationConfig(**MOCK_SERVER_1_CONFIG),
            mock_components_dict=components,
        )
        multi_server.get_integrations = MagicMock(
            return_value=[mock_airflow_integration]
        )
        multi_server.run_once()
        # Should start mock_server, should do 1 iteration
        assert len(multi_server.active_integrations) == 1
        assert len(multi_server.active_integrations[MOCK_SERVER_1_CONFIG["uid"]])

        multi_server.run_once()
        # Since components are created every single time, should create a new one and do 1 iteration
        assert len(multi_server.active_integrations) == 1
        assert len(multi_server.active_integrations[MOCK_SERVER_1_CONFIG["uid"]])

        multi_server.get_integrations.return_value = []
        multi_server.run_once()
        # should remove the server, don't do the additional iteration
        assert len(multi_server.active_integrations) == 0
        assert mock_airflow_integration.on_integration_disabled_call_count == 1
        assert not count_logged_exceptions(caplog)

    def test_05_test_error_cleanup(self, multi_server, caplog):

        mock_airflow_integration = MockAirflowIntegration(
            AirflowIntegrationConfig(**MOCK_SERVER_4_CONFIG)
        )
        multi_server.get_integrations = MagicMock(
            return_value=[mock_airflow_integration]
        )
        mock_airflow_integration.mock_reporting_service.error = "some_error"
        multi_server.run_once()
        multi_server.task_scheduler.wait_all_tasks()
        # should start mock_server, should do 1 iteration
        assert len(multi_server.active_integrations) == 1

        # On first run should clean existing error
        assert not mock_airflow_integration.reporting_service.error

        mock_airflow_integration.mock_data_fetcher.alive = False
        multi_server.run_once()
        multi_server.task_scheduler.wait_all_tasks()

        # still alive
        assert len(multi_server.active_integrations) == 1
        assert mock_airflow_integration.mock_reporting_service.error is not None

        first_error_lines = mock_airflow_integration.mock_reporting_service.error.split(
            "\n"
        )

        multi_server.run_once()
        multi_server.task_scheduler.wait_all_tasks()

        assert mock_airflow_integration.mock_reporting_service.error is not None
        new_error_lines = mock_airflow_integration.mock_reporting_service.error.split(
            "\n"
        )
        # should be same message except for last (Timestamp) line
        assert first_error_lines[:-1] == new_error_lines[:-1]

        mock_airflow_integration.mock_data_fetcher.alive = True
        multi_server.run_once()
        multi_server.task_scheduler.wait_all_tasks()

        assert len(multi_server.active_integrations) == 1
        assert not mock_airflow_integration.mock_reporting_service.error

    def test_06_report_metadata(self, multi_server):
        mock_airflow_integration = MockAirflowIntegration(
            AirflowIntegrationConfig(**MOCK_SERVER_1_CONFIG)
        )
        multi_server.get_integrations = MagicMock(
            return_value=[mock_airflow_integration]
        )
        plugin_metadata_dict = PluginMetadata(
            airflow_version="1.10.10",
            plugin_version="0.40.1 v2",
            airflow_instance_uid="34db92af-a525-522e-8f27-941cd4746d7b",
        ).as_dict()
        mock_airflow_integration.mock_adapter.metadata = plugin_metadata_dict

        # Refresh so that we will get plugin data
        multi_server.run_once()
        expected_metadata = plugin_metadata_dict.copy()
        expected_metadata["monitor_version"] = airflow_monitor.__version__
        assert (
            mock_airflow_integration.mock_reporting_service.metadata
            == expected_metadata
        )

    def test_07_syncer_last_heartbeat(self, multi_server):
        components = {"state_sync": MockSyncer}
        mock_airflow_integration = MockAirflowIntegration(
            AirflowIntegrationConfig(**MOCK_SERVER_1_CONFIG),
            mock_components_dict=components,
        )
        multi_server.get_integrations = MagicMock(
            return_value=[mock_airflow_integration]
        )
        multi_server.run_once()
        multi_server.task_scheduler.wait_all_tasks()
        # Should start mock_server, should do 1 iteration
        assert len(multi_server.active_integrations) == 1
        assert len(multi_server.active_integrations[MOCK_SERVER_1_CONFIG["uid"]]) == 1
        last_heartbeat = multi_server.active_integrations[MOCK_SERVER_1_CONFIG["uid"]][
            MockSyncer.SYNCER_TYPE
        ]
        assert last_heartbeat is not None

        multi_server.run_once()
        multi_server.task_scheduler.wait_all_tasks()

        new_last_heartbeat = multi_server.active_integrations[
            MOCK_SERVER_1_CONFIG["uid"]
        ][MockSyncer.SYNCER_TYPE]
        assert new_last_heartbeat is not None
        assert new_last_heartbeat != last_heartbeat

        # Now the interval is not met so shouldn't run again the component
        mock_airflow_integration.config.sync_interval = 10
        multi_server.run_once()
        newer_last_heartbeat = multi_server.active_integrations[
            MOCK_SERVER_1_CONFIG["uid"]
        ][MockSyncer.SYNCER_TYPE]
        assert newer_last_heartbeat == new_last_heartbeat

    def test_08_run_once_on_integration_enabled_raise_exception(self, multi_server):
        # Arrange
        multi_server.get_integrations = MagicMock()
        integration_with_error = MagicMock(
            config=_as_dotted_dict(**{"uid": get_uuid()})
        )
        integration_with_error.on_integration_enabled.side_effect = Exception("Error")
        integration = MagicMock(config=_as_dotted_dict(**{"uid": get_uuid()}))
        integrations = [integration_with_error, integration]
        multi_server.get_integrations.return_value = integrations
        # Act
        multi_server.run_once()

        # Assert
        assert len(multi_server.active_integrations) == 2

    def test_09_heartbeat_one_integration_raise_exception(self, multi_server):
        # Arrange
        existing_integration = MagicMock(config=_as_dotted_dict(**{"uid": get_uuid()}))
        missing_integration = MagicMock(config=_as_dotted_dict(**{"uid": get_uuid()}))
        multi_server.active_integrations = {existing_integration.config.uid: {}}
        existing_integration_component = MagicMock(spec=BaseComponent)
        missing_integration_component = MagicMock(spec=BaseComponent)
        existing_integration.get_components.return_value = [
            existing_integration_component
        ]
        missing_integration.get_components.return_value = [
            missing_integration_component
        ]

        # Act
        multi_server._heartbeat([missing_integration, existing_integration])
        multi_server.task_scheduler.wait_all_tasks()

        # Assert
        existing_integration_component.sync_once.assert_called_once()
        existing_integration_component.refresh_config.assert_called_once()

        missing_integration_component.sync_once.assert_not_called()
        missing_integration_component.refresh_config.assert_not_called()

    def test_10_heartbeat_continue_iteration_after_exception(self, multi_server):
        # Arrange
        first_integration = MagicMock(config=_as_dotted_dict(**{"uid": get_uuid()}))
        first_integration_component = MagicMock(spec=BaseComponent)
        first_integration.get_components.return_value = [first_integration_component]

        second_integration = MagicMock(config=_as_dotted_dict(**{"uid": get_uuid()}))
        second_integration_component = MagicMock(spec=BaseComponent)
        second_integration.get_components.return_value = [second_integration_component]
        second_integration.get_components.side_effect = Exception()

        third_integration = MagicMock(config=_as_dotted_dict(**{"uid": get_uuid()}))
        third_integration_component = MagicMock(spec=BaseComponent)
        third_integration.get_components.return_value = [third_integration_component]

        multi_server.active_integrations = {
            first_integration.config.uid: {},
            second_integration.config.uid: {},
            third_integration.config.uid: {},
        }

        try:
            # Act
            multi_server._heartbeat(
                [first_integration, second_integration, third_integration]
            )
            multi_server.task_scheduler.wait_all_tasks()
        except Exception:
            pytest.fail("Heartbeat should not raise an exception")

        # Assert
        first_integration.get_components.assert_called_once()
        second_integration.get_components.assert_called_once()
        third_integration.get_components.assert_called_once()

        first_integration_component.sync_once.assert_called_once()
        second_integration_component.sync_once.assert_not_called()
        third_integration_component.sync_once.assert_called_once()

    @patch("dbnd_monitor.multiserver.logger.warning")
    @patch("dbnd_monitor.multiserver.configure_sending_monitor_logs")
    def test_11_set_remote_log_handler(
        self, mock_configure_sending_monitor_logs, mock_logger, multi_server
    ):
        multi_server.set_remote_log_handler()
        mock_configure_sending_monitor_logs.assert_called_once()
        mock_logger.assert_not_called()

    @patch("dbnd_monitor.multiserver.logger.warning")
    @patch("dbnd_monitor.multiserver.configure_sending_monitor_logs")
    def test_12_set_remote_log_handler_with_multiple_integration_types(
        self,
        mock_configure_sending_monitor_logs,
        mock_logger,
        multi_server_with_multiple_integration_types,
    ):
        multi_server_with_multiple_integration_types.set_remote_log_handler()
        mock_configure_sending_monitor_logs.assert_not_called()
        mock_logger.assert_called_once_with(
            "This monitor contains more than one integrations type, configuring sending monitor logs not possible"
        )

    @patch(
        "airflow_monitor.multiserver.airflow_integration.get_or_create_airflow_instance_uid"
    )
    def test_get_integrations(self, mock_instance_uid, multi_server):
        test_uid = str(uuid.uuid4())
        mock_instance_uid.return_value = test_uid
        multi_server.integration_management_service.get_all_integration_configs = (
            MagicMock()
        )
        multi_server.get_integrations()
        multi_server.integration_management_service.get_all_integration_configs.assert_called_once_with(
            monitor_type="airflow", syncer_name=None, source_instance_uid=test_uid
        )
