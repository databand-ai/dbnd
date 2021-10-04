import random
import string

from functools import wraps

import pytest

from mock import MagicMock, patch

from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.multiserver.multiserver import MultiServerMonitor
from airflow_monitor.syncer.runtime_syncer import AirflowRuntimeSyncer
from airflow_monitor.tracking_service import get_servers_configuration_service
from dbnd.utils.api_client import ApiClient

from .conftest import WebAppTest


@pytest.fixture
def mock_sync_once():
    with patch.object(
        AirflowRuntimeSyncer,
        "_sync_once",
        new=mock_decorator(AirflowRuntimeSyncer._sync_once),
    ) as patched_sync_once:
        yield patched_sync_once.mock


def mock_decorator(method_to_decorate):
    mock = MagicMock()

    @wraps(method_to_decorate)
    def wrapper(*args, **kwargs):
        res = method_to_decorate(*args, **kwargs)
        mock(*args, **kwargs)
        return res

    wrapper.mock = mock
    return wrapper


class TestSyncerWorks(WebAppTest):
    def set_is_monitor_enabled(self, url, is_sync_enabled):
        self.client.post(
            self._url("AirflowServersApi.set_is_enabled"),
            json={"base_url": url, "is_enabled": is_sync_enabled},
        )

    def set_monitor_archived(self, url):
        self.client.post(self._url("AirflowServersApi.archive"), json=url)

    def set_monitor_unarchived(self, url):
        self.client.post(self._url("AirflowServersApi.unarchive"), json=url)

    def get_server_info(self, name):
        servers = self.client.get(self._url("AirflowServersApi.get_airflow_monitors"))
        for server in servers.json["data"]:
            if server["name"] == name:
                return server

    @pytest.fixture
    def syncer_name(self, _set_values):
        random_name = "".join(random.choice(string.ascii_letters) for _ in range(10))
        self.client.post(
            self._url("AirflowServersApi.add"),
            json={
                "base_url": random_name,
                "name": random_name,
                "fetcher": "web",
                "external_url": "",
                "composer_client_id": "",
                "api_mode": "rbac",
                "airflow_environment": "on_prem",
                "dag_ids": "",
            },
        )
        return random_name

    @pytest.fixture
    def multi_server(self, mock_data_fetcher, syncer_name):
        with patch(
            "airflow_monitor.multiserver.monitor_component_manager.get_data_fetcher",
            return_value=mock_data_fetcher,
        ), patch(
            "airflow_monitor.common.base_component.get_data_fetcher",
            return_value=mock_data_fetcher,
        ), self.patch_api_client():
            yield MultiServerMonitor(
                get_servers_configuration_service(),
                AirflowMonitorConfig(syncer_name=syncer_name),
            )

    def test_01_server_sync_enable_disable(
        self, multi_server, syncer_name, mock_sync_once
    ):
        server_info = self.get_server_info(syncer_name)
        assert server_info["last_sync_time"] is None

        multi_server.run_once()
        assert mock_sync_once.call_count == 1

        server_info = self.get_server_info(syncer_name)
        assert server_info["last_sync_time"] is not None
        last_sync_time = server_info["last_sync_time"]

        self.set_is_monitor_enabled(syncer_name, False)
        multi_server.run_once()
        assert mock_sync_once.call_count == 1

        server_info = self.get_server_info(syncer_name)
        assert server_info["last_sync_time"] == last_sync_time

        self.set_is_monitor_enabled(syncer_name, True)
        multi_server.run_once()
        assert mock_sync_once.call_count == 2

        server_info = self.get_server_info(syncer_name)
        assert server_info["last_sync_time"] > last_sync_time

    def test_02_server_archive_unarchive(
        self, multi_server, syncer_name, mock_sync_once
    ):
        multi_server.run_once()
        assert mock_sync_once.call_count == 1

        self.set_monitor_archived(syncer_name)
        multi_server.run_once()
        assert mock_sync_once.call_count == 1

        self.set_monitor_unarchived(syncer_name)
        multi_server.run_once()
        assert mock_sync_once.call_count == 2

    def test_03_source_instance_uid(
        self, multi_server, syncer_name, mock_sync_once, mock_data_fetcher
    ):
        mock_data_fetcher.airflow_version = "1.10.10"
        mock_data_fetcher.plugin_version = "0.40.1 v2"
        mock_data_fetcher.airflow_instance_uid = "34db92af-a525-522e-8f27-941cd4746d7b"

        server_info = self.get_server_info(syncer_name)
        assert (
            server_info["airflow_version"],
            server_info["airflow_export_version"],
            server_info["source_instance_uid"],
        ) == (None, None, None)

        multi_server.run_once()

        server_info = self.get_server_info(syncer_name)
        assert (
            server_info["airflow_version"],
            server_info["airflow_export_version"],
            server_info["source_instance_uid"],
        ) == ("1.10.10", "0.40.1 v2", "34db92af-a525-522e-8f27-941cd4746d7b")
