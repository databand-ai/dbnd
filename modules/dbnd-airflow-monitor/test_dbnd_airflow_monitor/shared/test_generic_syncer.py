# © Copyright Databand.ai, an IBM Company 2022
from typing import Dict, List
from unittest.mock import patch

import pytest

from airflow_monitor.shared.adapter.adapter import Adapter, AdapterData
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_syncer_management_service import (
    BaseSyncerManagementService,
)
from airflow_monitor.shared.base_tracking_service import BaseTrackingService
from airflow_monitor.shared.generic_syncer import GenericSyncer


class MockAdapter(Adapter):
    def __init__(self):
        self.cursor = 0

    def get_data(
        self, cursor: int, batch_size: int, next_page: int
    ) -> (Dict[str, object], List[str], str):
        if next_page is not None:
            return AdapterData(
                data={"data": self.cursor + next_page}, failed=[], next_page=None
            )
        self.cursor += 1
        return AdapterData(data={"data": self.cursor}, failed=[], next_page=1)

    def get_last_cursor(self) -> int:
        return self.cursor

    def update_data(self, to_update: List[str]) -> Dict[str, object]:
        if to_update:
            return {"data": to_update}
        else:
            return None


class MockTrackingService(BaseTrackingService):
    def __init__(self, monitor_type: str, tracking_source_uid: str):
        BaseTrackingService.__init__(self, monitor_type, tracking_source_uid)
        self.sent_data = []
        self.last_seen_run_id = None
        self.last_cursor = None
        self.last_state = None
        self.counter = 0
        self.error = None
        self.active_runs = None

    def save_tracking_data(self, full_data):
        self.sent_data.append(full_data)
        if self.error and self.counter == 1:
            raise self.error
        self.counter += 1

    def update_last_cursor(self, integration_id, state, data):
        self.last_cursor = data
        self.last_state = state

    def get_last_cursor_and_state(self) -> (int, str):
        return self.last_cursor, self.last_state

    def get_last_cursor(self, integration_id) -> int:
        return self.last_cursor

    def get_active_runs(self) -> list[dict]:
        return self.active_runs

    def set_error(self, error):
        self.error = error

    def set_active_runs(self, active_runs):
        self.active_runs = active_runs


class MockSyncersManagementService(BaseSyncerManagementService):
    def update_monitor_state(self, server_id, monitor_state):
        pass

    def update_last_sync_time(self, server_id):
        pass

    def set_running_monitor_state(self, server_id):
        pass

    def set_starting_monitor_state(self, server_id):
        pass


@pytest.fixture
def mock_tracking_service() -> MockTrackingService:
    yield MockTrackingService("integration", "12345")


@pytest.fixture
def mock_server_config() -> BaseServerConfig:
    yield BaseServerConfig(
        source_name="test_syncer",
        source_type="integration",
        tracking_source_uid="12345",
        sync_interval=10,
    )


@pytest.fixture
def mock_adapter() -> MockAdapter:
    yield MockAdapter()


@pytest.fixture
def mock_syncer_management_service() -> MockSyncersManagementService:
    yield MockSyncersManagementService("integration", BaseServerConfig)


@pytest.fixture
def generic_runtime_syncer(
    mock_tracking_service,
    mock_server_config,
    mock_syncer_management_service,
    mock_adapter,
):
    syncer = GenericSyncer(
        config=mock_server_config,
        tracking_service=mock_tracking_service,
        syncer_management_service=mock_syncer_management_service,
        adapter=mock_adapter,
    )
    with patch.object(syncer, "refresh_config", new=lambda *args: None), patch.object(
        syncer, "tracking_service", wraps=syncer.tracking_service
    ), patch.object(
        syncer, "syncer_management_service", wraps=syncer.syncer_management_service
    ):
        yield syncer


class TestGenericSyncer:
    def test_sync_get_data_with_pagination(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (1, "update")
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (2, "update")
        assert mock_tracking_service.sent_data == [
            {"data": 1},
            {"data": 2},
            {"data": 2},
            {"data": 3},
        ]

    def test_sync_get_data_exception_on_save_data(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        mock_tracking_service.set_error(Exception("test"))
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        generic_runtime_syncer.sync_once()
        # last cursor is not updated after failure
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        generic_runtime_syncer.sync_once()
        # call get data with same cursor before failure
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        assert mock_tracking_service.sent_data == [
            {"data": 1},
            {"data": 2},
            {"data": 2},
        ]

    def test_sync_get_and_update_data_with_pagination(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        mock_tracking_service.set_active_runs([5, 6, 7, 8, 9, 10])
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.sent_data == [
            {"data": [5, 6, 7, 8, 9, 10]},
            {"data": 1},
            {"data": 2},
        ]
