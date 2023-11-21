# © Copyright Databand.ai, an IBM Company 2022

import time
import uuid

import pytest

from click.testing import CliRunner

import airflow_monitor

from airflow_monitor.common.airflow_data import PluginMetadata
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.multiserver.cmd_liveness_probe import airflow_monitor_v2_alive
from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.shared.multiserver import MultiServerMonitor
from dbnd._core.utils.uid_utils import get_uuid
from test_dbnd_airflow_monitor.airflow_utils import TestConnectionError


MOCK_SERVER_1_CONFIG = {
    "uid": get_uuid(),
    "config_updater_enabled": False,
    "source_type": "airflow",
    "source_name": "mock_server_1",
    "tracking_source_uid": uuid.uuid4(),
    "sync_interval": 0,  # Important so that the same syncer can run for another iteration
}
MOCK_SERVER_2_CONFIG = {
    "uid": get_uuid(),
    "config_updater_enabled": False,
    "source_type": "airflow",
    "source_name": "mock_server_2",
    "tracking_source_uid": uuid.uuid4(),
}
MOCK_SERVER_3_CONFIG = {
    "uid": get_uuid(),
    "config_updater_enabled": False,
    "source_type": "airflow",
    "source_name": "mock_server_3",
    "tracking_source_uid": uuid.uuid4(),
    "state_sync_enabled": True,
}
MOCK_SERVER_4_CONFIG = {
    "uid": get_uuid(),
    "config_updater_enabled": False,
    "source_type": "airflow",
    "source_name": "mock_server_4",
    "tracking_source_uid": uuid.uuid4(),
    "state_sync_enabled": True,
    "sync_interval": 0,  # Important so that the same syncer can run for another iteration
}


@pytest.fixture
def airflow_monitor_config():
    # use dummy local_dag_folder to generate "new" config object
    # (that won't conflict with monitor v1 global configuration)
    return AirflowMonitorConfig(local_dag_folder="/tmp")


@pytest.fixture
def multi_server(
    mock_integration_management_service,
    mock_data_fetcher,
    mock_tracking_service,
    airflow_monitor_config,
    mock_airflow_services_factory,
):
    yield MultiServerMonitor(
        monitor_config=airflow_monitor_config,
        monitor_services_factory=mock_airflow_services_factory,
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

    def test_03_empty_config(
        self, multi_server, mock_integration_management_service, caplog
    ):
        # server config is empty (all components disabled) - nothing should run
        mock_integration_management_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_1_CONFIG),
            AirflowServerConfig(**MOCK_SERVER_2_CONFIG),
        ]
        multi_server.run_once()

        assert len(multi_server.active_integrations) == 2
        for components_dict in multi_server.active_integrations.values():
            # All components should be there regardless if they are disabled in the config (no longer supported feature)
            assert len(components_dict) == 3
        assert not count_logged_exceptions(caplog)

    def test_04_single_server_single_component(
        self,
        multi_server,
        mock_integration_management_service,
        mock_airflow_services_factory,
        caplog,
    ):
        components = {"state_sync": MockSyncer}
        mock_airflow_services_factory.mock_components_dict = components
        mock_integration_management_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_1_CONFIG, state_sync_enabled=True)
        ]
        multi_server.run_once()
        # Should start mock_server, should do 1 iteration
        assert len(multi_server.active_integrations) == 1
        assert len(multi_server.active_integrations[MOCK_SERVER_1_CONFIG["uid"]])

        multi_server.run_once()
        # Since components are created every single time, should create a new one and do 1 iteration
        assert len(multi_server.active_integrations) == 1
        assert len(multi_server.active_integrations[MOCK_SERVER_1_CONFIG["uid"]])

        mock_integration_management_service.mock_servers = []
        multi_server.run_once()
        # should remove the server, don't do the additional iteration
        assert len(multi_server.active_integrations) == 0
        assert mock_airflow_services_factory.on_integration_disabled_call_count == 1
        assert not count_logged_exceptions(caplog)

    def test_05_test_error_cleanup(
        self,
        multi_server,
        mock_integration_management_service,
        mock_data_fetcher,
        mock_tracking_service,
        mock_reporting_service,
        caplog,
    ):
        mock_integration_management_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_4_CONFIG)
        ]
        mock_reporting_service.error = "some_error"
        multi_server.run_once()
        # should start mock_server, should do 1 iteration
        assert len(multi_server.active_integrations) == 1

        # On first run should clean existing error
        assert not mock_reporting_service.error

        mock_data_fetcher.alive = False
        multi_server.run_once()
        # still alive
        assert len(multi_server.active_integrations) == 1
        assert mock_reporting_service.error is not None

        first_error_lines = mock_reporting_service.error.split("\n")

        multi_server.run_once()
        assert mock_reporting_service.error is not None
        new_error_lines = mock_reporting_service.error.split("\n")
        # should be same message except for last (Timestamp) line
        assert first_error_lines[:-1] == new_error_lines[:-1]

        mock_data_fetcher.alive = True
        multi_server.run_once()
        assert len(multi_server.active_integrations) == 1
        assert not mock_reporting_service.error

    def test_06_liveness_prove(
        self, multi_server, mock_integration_management_service, caplog
    ):
        runner = CliRunner()
        multi_server.run_once()

        result = runner.invoke(airflow_monitor_v2_alive, ["--max-time-diff", "5"])
        assert result.exit_code == 0

        time.sleep(6)
        result = runner.invoke(airflow_monitor_v2_alive, ["--max-time-diff", "5"])
        assert result.exit_code != 0

        multi_server.run_once()
        result = runner.invoke(airflow_monitor_v2_alive, ["--max-time-diff", "5"])
        assert result.exit_code == 0

    def test_07_report_metadata(
        self,
        multi_server,
        mock_data_fetcher,
        mock_airflow_services_factory,
        mock_integration_management_service,
        mock_reporting_service,
    ):
        mock_integration_management_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_1_CONFIG)
        ]
        plugin_metadata_dict = PluginMetadata(
            airflow_version="1.10.10",
            plugin_version="0.40.1 v2",
            airflow_instance_uid="34db92af-a525-522e-8f27-941cd4746d7b",
        ).as_dict()
        mock_airflow_services_factory.mock_adapter.metadata = plugin_metadata_dict

        # Refresh so that we will get plugin data
        multi_server.run_once()
        expected_metadata = plugin_metadata_dict.copy()
        expected_metadata["monitor_version"] = airflow_monitor.__version__
        assert mock_reporting_service.metadata == expected_metadata

    def test_08_syncer_last_heartbeat(
        self,
        multi_server,
        mock_integration_management_service,
        mock_airflow_services_factory,
    ):
        components = {"state_sync": MockSyncer}
        mock_airflow_services_factory.mock_components_dict = components
        config = AirflowServerConfig(**MOCK_SERVER_1_CONFIG, state_sync_enabled=True)
        mock_integration_management_service.mock_servers = [config]
        multi_server.run_once()
        # Should start mock_server, should do 1 iteration
        assert len(multi_server.active_integrations) == 1
        assert len(multi_server.active_integrations[MOCK_SERVER_1_CONFIG["uid"]]) == 1
        last_heartbeat = multi_server.active_integrations[MOCK_SERVER_1_CONFIG["uid"]][
            MockSyncer.SYNCER_TYPE
        ]
        assert last_heartbeat is not None

        multi_server.run_once()

        new_last_heartbeat = multi_server.active_integrations[
            MOCK_SERVER_1_CONFIG["uid"]
        ][MockSyncer.SYNCER_TYPE]
        assert new_last_heartbeat is not None
        assert new_last_heartbeat != last_heartbeat

        # Now the interval is not met so shouldn't run again the component
        config.sync_interval = 10
        multi_server.run_once()
        newer_last_heartbeat = multi_server.active_integrations[
            MOCK_SERVER_1_CONFIG["uid"]
        ][MockSyncer.SYNCER_TYPE]
        assert newer_last_heartbeat == new_last_heartbeat
