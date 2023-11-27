# © Copyright Databand.ai, an IBM Company 2022

from typing import List, Tuple
from unittest.mock import patch
from uuid import uuid4

import pytest

from attr import evolve

from airflow_monitor.shared.adapter.adapter import (
    Adapter,
    Assets,
    AssetState,
    AssetToState,
)
from airflow_monitor.shared.base_integration_config import BaseIntegrationConfig
from airflow_monitor.shared.base_tracking_service import BaseTrackingService
from airflow_monitor.shared.generic_syncer import GenericSyncer


INTEGRATION_UID = uuid4()


class MockAdapter(Adapter):
    def __init__(self):
        super(MockAdapter, self).__init__()
        self.cursor: int = 0
        self.error: Exception = None

    def set_error(self, error: Exception) -> None:
        self.error = error

    def get_new_assets_for_cursor(self, cursor: int) -> Tuple[Assets, int]:
        old_cursor = self.cursor
        self.cursor += 1
        return (
            Assets(
                data=None,
                assets_to_state=[
                    AssetToState(asset_id=old_cursor, state=AssetState.INIT)
                ],
            ),
            old_cursor,
        )

    def init_cursor(self) -> int:
        return self.cursor

    def get_assets_data(self, assets: Assets) -> Assets:
        if self.error:
            raise self.error
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
        self.error = None
        self.active_runs = None

    def save_tracking_data(self, assets_data):
        self.sent_data.append(assets_data)
        if self.error:
            raise self.error

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


@pytest.fixture
def mock_tracking_service() -> MockTrackingService:
    yield MockTrackingService("integration", "12345")


@pytest.fixture
def mock_config() -> BaseIntegrationConfig:
    yield BaseIntegrationConfig(
        uid=INTEGRATION_UID,
        source_name="test_syncer",
        source_type="integration",
        tracking_source_uid="12345",
        sync_interval=10,
    )


@pytest.fixture
def mock_adapter() -> MockAdapter:
    yield MockAdapter()


@pytest.fixture
def generic_runtime_syncer(
    mock_tracking_service, mock_config, mock_reporting_service, mock_adapter
):
    syncer = GenericSyncer(
        config=mock_config,
        tracking_service=mock_tracking_service,
        reporting_service=mock_reporting_service,
        adapter=mock_adapter,
        syncer_instance_id="123",
    )
    with patch.object(syncer, "refresh_config", new=lambda *args: None), patch.object(
        syncer, "tracking_service", wraps=syncer.tracking_service
    ), patch.object(syncer, "reporting_service", wraps=syncer.reporting_service):
        yield syncer
