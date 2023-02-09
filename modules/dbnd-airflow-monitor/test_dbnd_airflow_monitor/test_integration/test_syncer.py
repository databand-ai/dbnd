# © Copyright Databand.ai, an IBM Company 2022

import json
import random
import string

from functools import wraps

import pytest

from mock import MagicMock, patch

from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.config_updater.runtime_config_updater import (
    AirflowRuntimeConfigUpdater,
)
from airflow_monitor.multiserver.airflow_services_factory import AirflowServicesFactory
from airflow_monitor.shared.multiserver import MultiServerMonitor
from airflow_monitor.syncer.runtime_syncer import AirflowRuntimeSyncer

from .conftest import WebAppTest


@pytest.fixture
def mock_runtime_syncer_sync_once():
    with patch.object(
        AirflowRuntimeSyncer,
        "_sync_once",
        new=mock_decorator(AirflowRuntimeSyncer._sync_once),
    ) as patched_sync_once:
        yield patched_sync_once.mock


@pytest.fixture
def mock_config_updater_sync_once():
    with patch.object(
        AirflowRuntimeConfigUpdater,
        "_sync_once",
        new=mock_decorator(AirflowRuntimeConfigUpdater._sync_once),
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
    def set_is_monitor_enabled(self, uid, is_sync_enabled):
        self.client.post(
            self._url("AirflowServersApi.set_is_enabled"),
            json={"tracking_source_uid": uid, "is_enabled": is_sync_enabled},
        )

    def set_monitor_archived(self, uid):
        self.client.post(self._url("AirflowServersApi.archive"), json=uid)

    def set_monitor_unarchived(self, uid):
        self.client.post(self._url("AirflowServersApi.unarchive"), json=uid)

    def get_server_info_by_tracking_source_uid(self, tracking_source_uid):
        servers = self.client.get(self._url("AirflowServersApi.get_airflow_monitors"))
        for server in servers.json["data"]:
            if server["tracking_source_uid"] == tracking_source_uid:
                return server

    def get_server_info_by_wildcard(self, name):
        query_filter = {
            "filter": json.dumps([{"name": "*", "op": "ilike", "val": name}])
        }
        servers = self.client.get(
            self._url("AirflowServersApi.get_airflow_monitors"),
            query_string=query_filter,
        )
        return servers.json["data"]

    @pytest.fixture
    def syncer(self, _web_app_ctrl_with_login):
        random_name = "".join(random.choice(string.ascii_letters) for _ in range(10))
        random_host = "".join(random.choice(string.ascii_letters) for _ in range(10))
        af_server_info = {
            "base_url": f"https://{random_host}.local",
            "name": random_name,
            "fetcher": "db",
            "external_url": "",
            "composer_client_id": "",
            "api_mode": "rbac",
            "airflow_environment": "on_prem",
            "dag_ids": "",
            "monitor_config": {"include_sources": False, "is_sync_enabled": True},
        }
        created_syncer = self.client.post(
            self._url("AirflowServersApi.add"), json=af_server_info
        )

        syncer_dict = created_syncer.json

        return {
            "tracking_source_uid": syncer_dict["tracking_source_uid"],
            "name": random_name,
            "af_server_info": af_server_info,
            **syncer_dict["server_info_dict"],
        }

    @pytest.fixture
    def multi_server(self, mock_data_fetcher, syncer):
        with patch(
            "airflow_monitor.multiserver.airflow_services_factory.AirflowServicesFactory.get_data_fetcher",
            return_value=mock_data_fetcher,
        ), self.patch_api_client():
            syncer_name = syncer["name"]
            monitor_config = AirflowMonitorConfig(syncer_name=syncer_name)
            yield MultiServerMonitor(
                monitor_config=monitor_config,
                monitor_services_factory=AirflowServicesFactory(monitor_config),
            )

    def test_01_server_sync_enable_disable(
        self,
        multi_server,
        syncer,
        mock_runtime_syncer_sync_once,
        mock_config_updater_sync_once,
    ):
        tracking_source_uid = syncer["tracking_source_uid"]
        server_info = self.get_server_info_by_tracking_source_uid(tracking_source_uid)
        assert server_info["last_sync_time"] is None

        multi_server.run_once()
        assert mock_runtime_syncer_sync_once.call_count == 1

        server_info = self.get_server_info_by_tracking_source_uid(tracking_source_uid)
        assert server_info["last_sync_time"] is not None
        last_sync_time = server_info["last_sync_time"]

        self.set_is_monitor_enabled(tracking_source_uid, False)
        multi_server.run_once()
        assert mock_runtime_syncer_sync_once.call_count == 1

        server_info = self.get_server_info_by_tracking_source_uid(tracking_source_uid)
        assert server_info["last_sync_time"] == last_sync_time

        self.set_is_monitor_enabled(tracking_source_uid, True)
        multi_server.run_once()
        assert mock_runtime_syncer_sync_once.call_count == 2

        server_info = self.get_server_info_by_tracking_source_uid(tracking_source_uid)
        assert server_info["last_sync_time"] > last_sync_time

    def test_02_config_updater_sync_enable_disable(
        self,
        multi_server,
        syncer,
        mock_runtime_syncer_sync_once,
        mock_config_updater_sync_once,
    ):
        tracking_source_uid = syncer["tracking_source_uid"]
        server_info = self.get_server_info_by_tracking_source_uid(tracking_source_uid)
        assert server_info["last_sync_time"] is None

        multi_server.run_once()  # normal start, sync all
        assert mock_runtime_syncer_sync_once.call_count == 1
        assert mock_config_updater_sync_once.call_count == 1

        self.set_is_monitor_enabled(tracking_source_uid, False)
        multi_server.run_once()  # run only config updater once disabled, without runtime updater
        assert mock_runtime_syncer_sync_once.call_count == 1
        assert mock_config_updater_sync_once.call_count == 2

        self.set_is_monitor_enabled(tracking_source_uid, True)
        multi_server.run_once()  # run both once enabled
        assert mock_runtime_syncer_sync_once.call_count == 2
        assert mock_config_updater_sync_once.call_count == 3

        self.set_is_monitor_enabled(tracking_source_uid, False)
        multi_server.run_once()  # again run only config updater once disabled, without runtime updater
        assert mock_runtime_syncer_sync_once.call_count == 2
        assert mock_config_updater_sync_once.call_count == 4

    def test_03_server_archive_unarchive(
        self, multi_server, syncer, mock_runtime_syncer_sync_once, caplog
    ):
        tracking_source_uid = syncer["tracking_source_uid"]
        multi_server.run_once()
        assert mock_runtime_syncer_sync_once.call_count == 1

        self.set_monitor_archived(tracking_source_uid)

        multi_server.run_once()
        assert "No integrations found" in caplog.text

        assert mock_runtime_syncer_sync_once.call_count == 1

        self.set_monitor_unarchived(tracking_source_uid)

        multi_server.run_once()
        assert mock_runtime_syncer_sync_once.call_count == 2

    def test_get_syncer_search(self, syncer):
        syncer_name = syncer["name"]

        server_info = self.get_server_info_by_wildcard(syncer_name)

        assert len(server_info) == 1
        assert server_info[0]["name"] == syncer["af_server_info"]["name"]
        assert server_info[0]["base_url"] == syncer["af_server_info"]["base_url"]
