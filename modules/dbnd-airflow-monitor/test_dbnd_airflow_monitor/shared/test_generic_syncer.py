# © Copyright Databand.ai, an IBM Company 2022
from typing import Generator, List, Tuple
from unittest.mock import MagicMock, patch

import pytest

from attr import evolve

from airflow_monitor.shared.adapter.adapter import (
    Adapter,
    Assets,
    AssetState,
    AssetToState,
)
from airflow_monitor.shared.base_server_monitor_config import BaseServerConfig
from airflow_monitor.shared.base_tracking_service import BaseTrackingService
from airflow_monitor.shared.generic_syncer import GenericSyncer
from airflow_monitor.shared.integration_management_service import (
    IntegrationManagementService,
)
from dbnd._core.utils.uid_utils import get_uuid


class MockAdapter(Adapter):
    def __init__(self, config):
        super(MockAdapter, self).__init__(config)
        self.cursor = 0
        self.next_page = 0

    def init_assets_for_cursor(
        self, cursor: int, batch_size: int
    ) -> Generator[Tuple[Assets, int], None, None]:
        while True:
            yield (
                Assets(
                    data=None,
                    assets_to_state=[
                        AssetToState(
                            asset_id=self.cursor + self.next_page, state=AssetState.INIT
                        )
                    ],
                ),
                self.cursor,
            )
            self.cursor += 1
            if self.next_page == 1:
                break
            self.next_page = 1

    def init_cursor(self) -> int:
        return self.cursor

    def get_assets_data(self, assets: Assets) -> Assets:
        if assets.assets_to_state:
            return Assets(
                data={
                    "data": [
                        asset_to_state.asset_id
                        for asset_to_state in assets.assets_to_state
                    ]
                },
                assets_to_state=[
                    evolve(asset_to_state, state=AssetState.FAILED_REQUEST)
                    if asset_to_state.state == AssetState.FAILED_REQUEST
                    else evolve(asset_to_state, state=AssetState.FINISHED)
                    for asset_to_state in assets.assets_to_state
                ],
            )
        else:
            return Assets(data={}, assets_to_state=[])


class MockTrackingService(BaseTrackingService):
    def __init__(self, monitor_type: str, tracking_source_uid: str):
        BaseTrackingService.__init__(self, monitor_type, tracking_source_uid)
        self.sent_data = []
        self.assets_state = []
        self.last_seen_run_id = None
        self.last_cursor = None
        self.last_state = None
        self.counter = 0
        self.error = None
        self.active_runs = None

    def save_tracking_data(self, assets_data):
        self.sent_data.append(assets_data)
        if self.error and self.counter == 1:
            raise self.error
        self.counter += 1

    def save_assets_state(
        self, integration_id, syncer_instance_id, assets_to_state: List[AssetToState]
    ):
        self.assets_state.append(assets_to_state)
        return

    def update_last_cursor(self, integration_id, syncer_instance_id, state, data):
        self.last_cursor = data
        self.last_state = state

    def get_last_cursor_and_state(self) -> (int, str):
        return self.last_cursor, self.last_state

    def get_last_cursor(self, integration_id, syncer_instance_id) -> int:
        return self.last_cursor

    def get_active_assets(self, integration_id, syncer_instance_id) -> List[dict]:
        assets_to_state = []
        if self.active_runs:
            for asset_to_state_dict in self.active_runs:
                try:
                    assets_to_state.append(AssetToState.from_dict(asset_to_state_dict))
                except:
                    continue
            return assets_to_state

    def set_error(self, error):
        self.error = error

    def set_active_runs(self, active_runs):
        self.active_runs = active_runs


class MockIntegrationManagementService(IntegrationManagementService):
    def report_monitor_time_data(self, integration_uid, synced_new_data=False):
        pass

    def report_metadata(self, integration_uid, metadata):
        pass

    def report_error(self, integration_uid, full_function_name, err_message):
        pass


@pytest.fixture
def mock_tracking_service() -> MockTrackingService:
    yield MockTrackingService("integration", "12345")


@pytest.fixture
def mock_server_config() -> BaseServerConfig:
    yield BaseServerConfig(
        uid=get_uuid(),
        source_name="test_syncer",
        source_type="integration",
        tracking_source_uid="12345",
        sync_interval=10,
    )


@pytest.fixture
def mock_adapter() -> MockAdapter:
    yield MockAdapter(MagicMock())


@pytest.fixture
def mock_integration_management_service() -> MockIntegrationManagementService:
    yield MockIntegrationManagementService("integration", BaseServerConfig)


@pytest.fixture
def generic_runtime_syncer(
    mock_tracking_service,
    mock_server_config,
    mock_integration_management_service,
    mock_adapter,
):
    syncer = GenericSyncer(
        config=mock_server_config,
        tracking_service=mock_tracking_service,
        integration_management_service=mock_integration_management_service,
        adapter=mock_adapter,
        syncer_instance_id="123",
    )
    with patch.object(syncer, "refresh_config", new=lambda *args: None), patch.object(
        syncer, "tracking_service", wraps=syncer.tracking_service
    ), patch.object(
        syncer,
        "integration_management_service",
        wraps=syncer.integration_management_service,
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
            {"data": [0]},
            {"data": [2]},
            {"data": [3]},
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
            {"data": [0]},
            {"data": [2]},
            {"data": [2]},
        ]

    def test_sync_get_and_update_data_with_pagination(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        mock_tracking_service.set_active_runs(
            [
                {"asset_uri": 3, "state": "active"},
                {"asset_uri": 4, "state": "active"},
                {"asset_uri": 5, "state": "active"},
                {"asset_uri": 6, "state": "active"},
                {"asset_uri": 7, "state": "active"},
                {"asset_uri": 8, "state": "active"},
            ]
        )
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.assets_state == [
            [
                AssetToState(asset_id=3, state=AssetState.FINISHED),
                AssetToState(asset_id=4, state=AssetState.FINISHED),
                AssetToState(asset_id=5, state=AssetState.FINISHED),
                AssetToState(asset_id=6, state=AssetState.FINISHED),
                AssetToState(asset_id=7, state=AssetState.FINISHED),
                AssetToState(asset_id=8, state=AssetState.FINISHED),
            ],
            [AssetToState(asset_id=0, state=AssetState.FINISHED)],
            [AssetToState(asset_id=2, state=AssetState.FINISHED)],
        ]
        assert mock_tracking_service.sent_data == [
            {"data": [3, 4, 5, 6, 7, 8]},
            {"data": [0]},
            {"data": [2]},
        ]

    def test_sync_get_and_update_data_with_pagination_and_uknown_state(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        mock_tracking_service.set_active_runs(
            [
                {"asset_uri": 3, "state": "wow", "data": {"retry_count": 2}},
                {"asset_uri": 4, "state": "active"},
                {"asset_uri": 5, "state": "active"},
                {"asset_uri": 6, "state": "active"},
                {"asset_uri": 7, "state": "active"},
                {"asset_uri": 8, "state": "active"},
            ]
        )
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.assets_state == [
            [
                AssetToState(asset_id=4, state=AssetState.FINISHED),
                AssetToState(asset_id=5, state=AssetState.FINISHED),
                AssetToState(asset_id=6, state=AssetState.FINISHED),
                AssetToState(asset_id=7, state=AssetState.FINISHED),
                AssetToState(asset_id=8, state=AssetState.FINISHED),
            ],
            [AssetToState(asset_id=0, state=AssetState.FINISHED)],
            [AssetToState(asset_id=2, state=AssetState.FINISHED)],
        ]
        assert mock_tracking_service.sent_data == [
            {"data": [4, 5, 6, 7, 8]},
            {"data": [0]},
            {"data": [2]},
        ]

    def test_sync_get_and_update_data_with_pagination_and_failed_request(
        self,
        generic_runtime_syncer: GenericSyncer,
        mock_tracking_service: MockTrackingService,
    ):
        mock_tracking_service.set_active_runs(
            [
                {"asset_uri": 3, "state": "failed_request", "data": {"retry_count": 1}},
                {"asset_uri": 4, "state": "active"},
                {"asset_uri": 5, "state": "active"},
                {"asset_uri": 6, "state": "active"},
                {"asset_uri": 7, "state": "active"},
                {"asset_uri": 8, "state": "active"},
            ]
        )
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.get_last_cursor_and_state() == (0, "init")
        generic_runtime_syncer.sync_once()
        assert mock_tracking_service.assets_state == [
            [
                AssetToState(
                    asset_id=3, state=AssetState.FAILED_REQUEST, retry_count=2
                ),
                AssetToState(asset_id=4, state=AssetState.FINISHED),
                AssetToState(asset_id=5, state=AssetState.FINISHED),
                AssetToState(asset_id=6, state=AssetState.FINISHED),
                AssetToState(asset_id=7, state=AssetState.FINISHED),
                AssetToState(asset_id=8, state=AssetState.FINISHED),
            ],
            [AssetToState(asset_id=0, state=AssetState.FINISHED)],
            [AssetToState(asset_id=2, state=AssetState.FINISHED)],
        ]
