import uuid

import pytest

from mock import patch

from airflow_monitor.common import AirflowServerConfig, MultiServerMonitorConfig
from airflow_monitor.multiserver.multiserver import KNOWN_COMPONENTS, MultiServerMonitor
from airflow_monitor.syncer.base_syncer import BaseAirflowSyncer
from test_dbnd_airflow_monitor.airflow_utils import TestConnectionError


@pytest.fixture
def multi_server(mock_server_config_service, mock_data_fetcher):
    with patch(
        "airflow_monitor.multiserver.multiserver.get_data_fetcher",
        return_value=mock_data_fetcher,
    ):
        yield MultiServerMonitor(mock_server_config_service, MultiServerMonitorConfig())


@pytest.fixture
def mock_syncer_factory(mock_data_fetcher, mock_tracking_service):
    yield lambda: MockSyncer(
        config=mock_tracking_service.get_monitor_configuration(),
        data_fetcher=mock_data_fetcher,
        tracking_service=mock_tracking_service,
    )


@pytest.fixture
def mock_syncer(mock_syncer_factory):
    yield mock_syncer_factory()


def count_logged_exceptions(caplog):
    logged_exceptions = [record for record in caplog.records if record.exc_info]
    return len(logged_exceptions)


class MockSyncer(BaseAirflowSyncer):
    def __init__(self, *args, **kwargs):
        super(MockSyncer, self).__init__(*args, **kwargs)
        self.sync_count = 0
        self.should_fail = False

    def sync_once(self):
        if self.should_fail:
            raise Exception()
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
            AirflowServerConfig(uuid.uuid4()),
            AirflowServerConfig(uuid.uuid4()),
        ]
        multi_server.run_once()

        assert len(multi_server.active_monitors) == 2
        for monitor in multi_server.active_monitors.values():
            assert not monitor.active_components
        assert not count_logged_exceptions(caplog)

    def test_04_single_server_single_component(
        self, multi_server, mock_server_config_service, mock_syncer, caplog
    ):
        with patch.dict(
            KNOWN_COMPONENTS, {"state_sync": mock_syncer.emulate_start_syncer}
        ):
            mock_server_config_service.mock_servers = [
                AirflowServerConfig(uuid.uuid4(), state_sync_enabled=True)
            ]
            multi_server.run_once()
            # should start mock_server, should do 1 iteration
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer.sync_count == 1

            multi_server.run_once()
            # should not start additional servers, should do 1 more iteration
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer.sync_count == 2

            mock_server_config_service.mock_servers = []
            multi_server.run_once()
            # should remove the server, don't do the additional iteration
            assert len(multi_server.active_monitors) == 0
            assert mock_syncer.sync_count == 2
        assert not count_logged_exceptions(caplog)

    def test_05_failing_syncer(
        self, multi_server, mock_server_config_service, mock_syncer_factory, caplog
    ):
        mock_syncer1 = mock_syncer_factory()
        mock_syncer2 = mock_syncer_factory()

        mock_server_config_service.mock_servers = [
            AirflowServerConfig(uuid.uuid4(), state_sync_enabled=True)
        ]
        with patch.dict(
            KNOWN_COMPONENTS, {"state_sync": mock_syncer1.emulate_start_syncer}
        ):
            multi_server.run_once()
            # should start mock_server, should do 1 iteration
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer1.sync_count == 1

        with patch.dict(
            KNOWN_COMPONENTS, {"state_sync": mock_syncer2.emulate_start_syncer}
        ):
            # ensure it's not restarted (just because we've change component definition)
            multi_server.run_once()
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer1.sync_count == 2
            assert mock_syncer2.sync_count == 0
            assert not count_logged_exceptions(caplog)

            mock_syncer1.should_fail = True
            multi_server.run_once()
            # should not start additional servers, no new iteration
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer1.sync_count == 2
            assert mock_syncer2.sync_count == 0
            # we should expect here log message that syncer failed
            assert count_logged_exceptions(caplog)

            multi_server.run_once()
            # should restart the server
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer1.sync_count == 2
            assert mock_syncer2.sync_count == 1

        assert count_logged_exceptions(caplog) < 2

    def test_06_airflow_not_responsive(
        self,
        multi_server,
        mock_data_fetcher,
        mock_server_config_service,
        mock_syncer,
        caplog,
    ):
        mock_server_config_service.mock_servers = [
            AirflowServerConfig(uuid.uuid4(), state_sync_enabled=True)
        ]
        with patch.dict(
            KNOWN_COMPONENTS, {"state_sync": mock_syncer.emulate_start_syncer}
        ):
            mock_data_fetcher.alive = False
            multi_server.run_once()
            # should not start mock_server - airflow is not responding
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer.sync_count == 0

            mock_data_fetcher.alive = True
            multi_server.run_once()
            # should start now since it's alive
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer.sync_count == 1

            mock_data_fetcher.alive = False
            multi_server.run_once()
            # shouldn't actively kill the syncer, despite data_fetcher not responsive
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer.sync_count == 2
            for monitor in multi_server.active_monitors.values():
                assert monitor.active_components

            mock_syncer.should_fail = True
            multi_server.run_once()
            # now only if syncer fails (as a result of failing data_fetcher), it will be evicted
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer.sync_count == 2
            for monitor in multi_server.active_monitors.values():
                assert not monitor.active_components

            multi_server.run_once()
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer.sync_count == 2
            for monitor in multi_server.active_monitors.values():
                assert not monitor.active_components

            mock_syncer.should_fail = False
            mock_data_fetcher.alive = True
            # now if everything is ok - should be back
            multi_server.run_once()
            assert len(multi_server.active_monitors) == 1
            assert mock_syncer.sync_count == 3  # due to test - it should be 3 and not 1
            for monitor in multi_server.active_monitors.values():
                assert monitor.active_components

        # we should have only one exception (from failed syncer)
        assert count_logged_exceptions(caplog) < 2
