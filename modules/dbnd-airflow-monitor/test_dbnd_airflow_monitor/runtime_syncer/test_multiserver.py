# © Copyright Databand.ai, an IBM Company 2022

import time
import uuid

import pytest

from click.testing import CliRunner
from mock import patch

from airflow_monitor.common.base_component import BaseMonitorSyncer
from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.config_updater.runtime_config_updater import (
    AirflowRuntimeConfigUpdater,
)
from airflow_monitor.fixer.runtime_fixer import AirflowRuntimeFixer
from airflow_monitor.multiserver.cmd_liveness_probe import airflow_monitor_v2_alive
from airflow_monitor.multiserver.monitor_component_manager import (
    AirflowMonitorComponentManager,
)
from airflow_monitor.multiserver.multiserver import AirflowMultiServerMonitor
from airflow_monitor.syncer.runtime_syncer import AirflowRuntimeSyncer
from test_dbnd_airflow_monitor.airflow_utils import TestConnectionError


MOCK_SERVER_1_CONFIG = {
    "config_updater_enabled": False,
    "source_type": "airflow",
    "source_name": "mock_server_1",
    "tracking_source_uid": uuid.uuid4(),
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
}


@pytest.fixture
def airflow_monitor_config():
    # use dummy local_dag_folder to generate "new" config object
    # (that won't conflict with monitor v1 global configuration)
    return AirflowMonitorConfig(local_dag_folder="/tmp")


@pytest.fixture
def multi_server(
    mock_server_config_service,
    mock_data_fetcher,
    mock_tracking_service,
    airflow_monitor_config,
    mock_airflow_services_factory,
):
    with patch(
        "airflow_monitor.multiserver.monitor_component_manager.AirflowMonitorComponentManager._get_tracking_errors",
        return_value=None,
    ):
        yield AirflowMultiServerMonitor(
            monitor_component_manager=AirflowMonitorComponentManager,
            monitor_config=airflow_monitor_config,
            components_dict={
                "state_sync": AirflowRuntimeSyncer,
                "fixer": AirflowRuntimeFixer,
                "config_updater": AirflowRuntimeConfigUpdater,
            },
            monitor_services_factory=mock_airflow_services_factory,
        )


def count_logged_exceptions(caplog):
    logged_exceptions = [record for record in caplog.records if record.exc_info]
    return len(logged_exceptions)


class MockSyncer(BaseMonitorSyncer):
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


class MockSyncer2(BaseMonitorSyncer):
    def __init__(self, *args, **kwargs):
        super(MockSyncer2, self).__init__(*args, **kwargs)
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
        assert not multi_server.active_monitors

    def test_02_config_service_not_available(
        self, multi_server, mock_server_config_service
    ):
        mock_server_config_service.alive = False
        with pytest.raises(TestConnectionError):
            multi_server.run_once()

    def test_03_empty_config(self, multi_server, mock_server_config_service, caplog):
        # server config is empty (all components disabled) - nothing should run
        mock_server_config_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_1_CONFIG),
            AirflowServerConfig(**MOCK_SERVER_2_CONFIG),
        ]
        multi_server.run_once()

        assert len(multi_server.active_monitors) == 2
        for monitor in multi_server.active_monitors.values():
            assert not monitor.active_components
        assert not count_logged_exceptions(caplog)

    def test_04_single_server_single_component(
        self, multi_server, mock_server_config_service, caplog
    ):
        components = {"state_sync": MockSyncer}
        multi_server.components_dict = components
        mock_server_config_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_1_CONFIG, state_sync_enabled=True)
        ]
        multi_server.run_once()
        # should start mock_server, should do 1 iteration
        assert len(multi_server.active_monitors) == 1
        syncer_instance = (
            multi_server.active_monitors[MOCK_SERVER_1_CONFIG["tracking_source_uid"]]
            .active_components["state_sync"]
            .target
        )
        assert syncer_instance.sync_count == 1

        multi_server.run_once()
        # should not start additional servers, should do 1 more iteration
        assert len(multi_server.active_monitors) == 1
        assert syncer_instance.sync_count == 2

        mock_server_config_service.mock_servers = []
        multi_server.run_once()
        # should remove the server, don't do the additional iteration
        assert len(multi_server.active_monitors) == 0
        assert syncer_instance.sync_count == 2
        assert not count_logged_exceptions(caplog)

    def test_05_failing_syncer(
        self, multi_server, mock_server_config_service, mock_tracking_service, caplog
    ):
        mock_syncer1 = MockSyncer
        mock_syncer2 = MockSyncer2

        mock_server_config_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_1_CONFIG, state_sync_enabled=True)
        ]
        components = {"state_sync": mock_syncer1}
        multi_server.components_dict = components
        multi_server.run_once()
        # should start mock_server, should do 1 iteration
        assert len(multi_server.active_monitors) == 1
        syncer_instance1 = (
            multi_server.active_monitors[MOCK_SERVER_1_CONFIG["tracking_source_uid"]]
            .active_components["state_sync"]
            .target
        )
        assert type(syncer_instance1) == mock_syncer1
        assert syncer_instance1.sync_count == 1
        assert mock_tracking_service.current_monitor_state.monitor_status == "Running"

        components = {"state_sync": mock_syncer2}
        multi_server.active_monitors[
            MOCK_SERVER_1_CONFIG["tracking_source_uid"]
        ].services_components = components
        # ensure it's not restarted (just because we've change component definition)
        multi_server.run_once()
        assert len(multi_server.active_monitors) == 1
        assert syncer_instance1.sync_count == 2
        assert not count_logged_exceptions(caplog)
        assert not mock_tracking_service.current_monitor_state.monitor_error_message

        syncer_instance1.should_fail = True
        multi_server.run_once()
        # should not start additional servers, no new iteration
        assert len(multi_server.active_monitors) == 1
        assert syncer_instance1.sync_count == 2
        # we should expect here log message that syncer failed
        assert count_logged_exceptions(caplog)
        # we should expect error reported to webserver
        assert (
            "Traceback"
            in mock_tracking_service.current_monitor_state.monitor_error_message
        )

        multi_server.run_once()
        # should restart the server
        syncer_instance2 = (
            multi_server.active_monitors[MOCK_SERVER_1_CONFIG["tracking_source_uid"]]
            .active_components["state_sync"]
            .target
        )
        assert type(syncer_instance2) == MockSyncer2
        assert len(multi_server.active_monitors) == 1
        assert syncer_instance1.sync_count == 2
        assert syncer_instance2.sync_count == 1
        # should clean the error
        assert not mock_tracking_service.current_monitor_state.monitor_error_message

        assert count_logged_exceptions(caplog) < 2

    def test_06_airflow_not_responsive(
        self, multi_server, mock_data_fetcher, mock_server_config_service, caplog
    ):
        mock_server_config_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_3_CONFIG)
        ]
        components = {"state_sync": MockSyncer}
        multi_server.components_dict = components
        mock_data_fetcher.alive = False
        multi_server.run_once()
        # should not start mock_server - airflow is not responding
        assert len(multi_server.active_monitors) == 1

        assert (
            len(
                multi_server.active_monitors[
                    MOCK_SERVER_3_CONFIG["tracking_source_uid"]
                ].active_components
            )
            == 0
        )

        mock_data_fetcher.alive = True
        multi_server.run_once()
        # should start now since it's alive
        assert len(multi_server.active_monitors) == 1
        syncer_instance = (
            multi_server.active_monitors[MOCK_SERVER_3_CONFIG["tracking_source_uid"]]
            .active_components["state_sync"]
            .target
        )
        assert syncer_instance.sync_count == 1

        mock_data_fetcher.alive = False
        multi_server.run_once()
        # shouldn't actively kill the syncer, despite data_fetcher not responsive
        assert len(multi_server.active_monitors) == 1
        assert syncer_instance.sync_count == 2
        for monitor in multi_server.active_monitors.values():
            assert monitor.active_components

        syncer_instance.should_fail = True
        multi_server.run_once()
        # now only if syncer fails (as a result of failing data_fetcher), it will be evicted
        assert len(multi_server.active_monitors) == 1
        assert syncer_instance.sync_count == 2
        for monitor in multi_server.active_monitors.values():
            assert not monitor.active_components

        multi_server.run_once()
        assert len(multi_server.active_monitors) == 1
        assert syncer_instance.sync_count == 2
        for monitor in multi_server.active_monitors.values():
            assert not monitor.active_components

        syncer_instance.should_fail = False
        mock_data_fetcher.alive = True
        # now if everything is ok - should be back
        multi_server.run_once()
        assert len(multi_server.active_monitors) == 1
        new_syncer_instance = (
            multi_server.active_monitors[MOCK_SERVER_3_CONFIG["tracking_source_uid"]]
            .active_components["state_sync"]
            .target
        )
        assert new_syncer_instance != syncer_instance
        assert syncer_instance.sync_count == 2
        assert new_syncer_instance.sync_count == 1
        for monitor in multi_server.active_monitors.values():
            assert monitor.active_components

        # we should have only one exception (from failed syncer)
        assert count_logged_exceptions(caplog) < 2

    def test_07_test_error_cleanup(
        self,
        multi_server,
        mock_server_config_service,
        mock_data_fetcher,
        mock_tracking_service,
        caplog,
    ):
        mock_server_config_service.mock_servers = [
            AirflowServerConfig(**MOCK_SERVER_4_CONFIG)
        ]

        multi_server.run_once()
        # should start mock_server, should do 1 iteration
        assert len(multi_server.active_monitors) == 1
        assert mock_tracking_service.current_monitor_state.monitor_status == "Running"
        assert not mock_tracking_service.current_monitor_state.monitor_error_message

        mock_data_fetcher.alive = False
        multi_server.run_once()
        # still alive
        assert len(multi_server.active_monitors) == 1
        assert mock_tracking_service.current_monitor_state.monitor_status == "Running"
        assert mock_tracking_service.current_monitor_state.monitor_error_message

        first_error_lines = (
            mock_tracking_service.current_monitor_state.monitor_error_message.split(
                "\n"
            )
        )

        multi_server.run_once()
        assert mock_tracking_service.current_monitor_state.monitor_error_message
        new_error_lines = (
            mock_tracking_service.current_monitor_state.monitor_error_message.split(
                "\n"
            )
        )
        # should be same message except for last (Timestamp) line
        assert first_error_lines[:-1] == new_error_lines[:-1]

        mock_data_fetcher.alive = True
        multi_server.run_once()
        assert len(multi_server.active_monitors) == 1
        assert mock_tracking_service.current_monitor_state.monitor_status == "Running"
        assert not mock_tracking_service.current_monitor_state.monitor_error_message

    def test_08_liveness_prove(self, multi_server, mock_server_config_service, caplog):
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
