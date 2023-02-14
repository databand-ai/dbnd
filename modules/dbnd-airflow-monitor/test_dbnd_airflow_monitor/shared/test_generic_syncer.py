# © Copyright Databand.ai, an IBM Company 2022

from unittest.mock import patch

import pytest

from airflow_monitor.shared.adapter.adapter import Adapter
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
    ) -> (dict[str, object], list[str], str):
        if next_page is not None:
            return {"data": self.cursor + next_page}, [], None
        self.cursor += 1
        return {"data": self.cursor}, [], 1

    def get_last_cursor(self) -> int:
        return self.cursor


class MockTrackingService(BaseTrackingService):
    def __init__(self, monitor_type: str, tracking_source_uid: str):
        BaseTrackingService.__init__(self, monitor_type, tracking_source_uid)
        self.last_init_data = []
        self.last_seen_run_id = None
        self.last_seen_date = None

    def save_tracking_data(self, full_data):
        self.last_init_data = full_data

    def update_last_seen_values(self, date):
        self.last_seen_date = date

    def get_last_seen_date(self):
        return self.last_seen_date


class MockErrorTrackingService(BaseTrackingService):
    def __init__(self, monitor_type: str, tracking_source_uid: str):
        BaseTrackingService.__init__(self, monitor_type, tracking_source_uid)
        self.last_init_data = []
        self.last_seen_run_id = None
        self.last_seen_date = None
        self.counter = 0

    def save_tracking_data(self, full_data):
        self.last_init_data = full_data
        if self.counter == 1:
            raise Exception("error")
        self.counter += 1

    def update_last_seen_values(self, date):
        self.last_seen_date = date

    def get_last_seen_date(self):
        return self.last_seen_date


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
def mock_error_tracking_service() -> MockTrackingService:
    yield MockErrorTrackingService("integration", "12345")


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


@pytest.fixture
def generic_error_runtime_syncer(
    mock_error_tracking_service,
    mock_server_config,
    mock_syncer_management_service,
    mock_adapter,
):
    syncer = GenericSyncer(
        config=mock_server_config,
        tracking_service=mock_error_tracking_service,
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
    def test_sync_data_with_pagination(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        generic_runtime_syncer.sync_once()
        assert 0 == mock_tracking_service.get_last_seen_date()
        generic_runtime_syncer.sync_once()
        assert {"data": 2} == mock_tracking_service.last_init_data
        assert 1 == mock_tracking_service.get_last_seen_date()
        generic_runtime_syncer.sync_once()
        assert {"data": 3} == mock_tracking_service.last_init_data
        assert 2 == mock_tracking_service.get_last_seen_date()

    def test_sync_data_exception_on_save_data(
        self,
        generic_error_runtime_syncer: GenericSyncer,
        mock_error_tracking_service: MockTrackingService,
    ):
        generic_error_runtime_syncer.sync_once()
        assert 0 == mock_error_tracking_service.get_last_seen_date()
        generic_error_runtime_syncer.sync_once()
        assert {"data": 2} == mock_error_tracking_service.last_init_data
        # last cursor is not updated after failure
        assert 0 == mock_error_tracking_service.get_last_seen_date()
        # call get data with same cursor before failure
        generic_error_runtime_syncer.sync_once()
        assert {"data": 2} == mock_error_tracking_service.last_init_data
        assert 0 == mock_error_tracking_service.get_last_seen_date()
