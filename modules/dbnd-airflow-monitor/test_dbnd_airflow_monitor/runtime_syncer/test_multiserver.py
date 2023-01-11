# © Copyright Databand.ai, an IBM Company 2022

import time
import uuid

import pytest

from click.testing import CliRunner
from mock import patch

from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.multiserver.cmd_liveness_probe import airflow_monitor_v2_alive
from airflow_monitor.shared.base_component import BaseComponent
from airflow_monitor.shared.multiserver import MultiServerMonitor
from test_dbnd_airflow_monitor.airflow_utils import TestConnectionError


MOCK_SERVER_1_CONFIG = {
    "config_updater_enabled": False,
    "source_type": "airflow",
    "source_name": "mock_server_1",
    "tracking_source_uid": uuid.uuid4(),
    "sync_interval": 0,  # Important so that the same syncer can run for another iteration
}
MOCK_SERVER_2_CONFIG = {
    "config_updater_enabled": False,
    "source_type": "airflow",
    "source_name": "mock_server_2",
    "tracking_source_uid": uuid.uuid4(),
}
MOCK_SERVER_3_CONFIG = {
    "config_updater_enabled": False,
    "source_type": "airflow",
    "source_name": "mock_server_3",
    "tracking_source_uid": uuid.uuid4(),
    "state_sync_enabled": True,
}
MOCK_SERVER_4_CONFIG = {
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
    mock_syncer_management_service,
    mock_data_fetcher,
    mock_tracking_service,
    airflow_monitor_config,
    mock_airflow_services_factory,
):
    with patch(
        "airflow_monitor.tracking_service.airflow_syncer_management_service._get_tracking_errors",
        return_value=None,
    ):
        yield MultiServerMonitor(
            monitor_config=airflow_monitor_config,
            monitor_services_factory=mock_airflow_services_factory,
        )


def count_logged_exceptions(caplog):
    logged_exceptions = [record for record in caplog.records if record.exc_info]
    return len(logged_exceptions)


class MockSyncer(BaseComponent):
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
        assert not multi_server.active_instances

    def test_02_config_service_not_available(
        self, multi_server, mock_syncer_management_service
    ):
        mock_syncer_management_service.alive = False
        with pytest.raises(TestConnectionError):
            multi_server.run_once()

    def test_03_empty_config(
        self, multi_server, mock_syncer_management_service, caplog
    ):
        # server config is empty (all components disabled) - nothing should run
        mock_syncer_management_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_1_CONFIG),
            AirflowServerConfig(**MOCK_SERVER_2_CONFIG),
        ]
        multi_server.run_once()

        assert len(multi_server.active_instances) == 2
        for components_dict in multi_server.active_instances.values():
            # All components should be there regardless if they are disabled in the config (no longer supported feature)
            assert len(components_dict) == 3
        assert not count_logged_exceptions(caplog)

    def test_04_single_server_single_component(
        self,
        multi_server,
        mock_syncer_management_service,
        mock_airflow_services_factory,
        caplog,
    ):
        components = {"state_sync": MockSyncer}
        mock_airflow_services_factory.mock_components_dict = components
        mock_syncer_management_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_1_CONFIG, state_sync_enabled=True)
        ]
        multi_server.run_once()
        # should start mock_server, should do 1 iteration
        assert len(multi_server.active_instances) == 1
        syncer_instance = multi_server.active_instances[
            MOCK_SERVER_1_CONFIG["tracking_source_uid"]
        ][0]
        assert syncer_instance.sync_count == 1

        multi_server.run_once()
        # should not start additional servers, should do 1 more iteration
        assert len(multi_server.active_instances) == 1
        assert syncer_instance.sync_count == 2

        mock_syncer_management_service.mock_servers = []
        multi_server.run_once()
        # should remove the server, don't do the additional iteration
        assert len(multi_server.active_instances) == 0
        assert syncer_instance.sync_count == 2
        assert not count_logged_exceptions(caplog)

    def test_05_test_error_cleanup(
        self,
        multi_server,
        mock_syncer_management_service,
        mock_data_fetcher,
        mock_tracking_service,
        caplog,
    ):
        mock_syncer_management_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_4_CONFIG)
        ]
        server_id = MOCK_SERVER_4_CONFIG["tracking_source_uid"]

        multi_server.run_once()
        # should start mock_server, should do 1 iteration
        assert len(multi_server.active_instances) == 1
        assert (
            mock_syncer_management_service.get_current_monitor_state(
                server_id
            ).monitor_status
            == "Running"
        )
        assert not mock_syncer_management_service.get_current_monitor_state(
            server_id
        ).monitor_error_message

        mock_data_fetcher.alive = False
        multi_server.run_once()
        # still alive
        assert len(multi_server.active_instances) == 1
        assert (
            mock_syncer_management_service.get_current_monitor_state(
                server_id
            ).monitor_status
            == "Running"
        )
        assert mock_syncer_management_service.get_current_monitor_state(
            server_id
        ).monitor_error_message

        first_error_lines = mock_syncer_management_service.get_current_monitor_state(
            server_id
        ).monitor_error_message.split("\n")

        multi_server.run_once()
        assert mock_syncer_management_service.get_current_monitor_state(
            server_id
        ).monitor_error_message
        new_error_lines = mock_syncer_management_service.get_current_monitor_state(
            server_id
        ).monitor_error_message.split("\n")
        # should be same message except for last (Timestamp) line
        assert first_error_lines[:-1] == new_error_lines[:-1]

        mock_data_fetcher.alive = True
        multi_server.run_once()
        assert len(multi_server.active_instances) == 1
        assert (
            mock_syncer_management_service.get_current_monitor_state(
                server_id
            ).monitor_status
            == "Running"
        )
        assert not mock_syncer_management_service.get_current_monitor_state(
            server_id
        ).monitor_error_message

    def test_06_liveness_prove(
        self, multi_server, mock_syncer_management_service, caplog
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
