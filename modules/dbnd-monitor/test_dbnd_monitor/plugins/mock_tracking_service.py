# © Copyright Databand.ai, an IBM Company 2022
from typing import List, Tuple

import pytest

from dbnd._vendor.tenacity import retry, stop_after_attempt
from dbnd_monitor.adapter import AssetToState
from dbnd_monitor.base_tracking_service import BaseTrackingService


class MockTrackingService(BaseTrackingService):
    def __init__(self, monitor_type: str, tracking_source_uid: str):
        super().__init__(monitor_type, tracking_source_uid)
        self.sent_data = []
        self.assets_state = []
        self.last_seen_run_id = None
        self.last_cursor = None
        self.last_state = None
        self.error = None
        self.active_runs = None

    @retry(stop=stop_after_attempt(2), reraise=True)
    def save_tracking_data(self, assets_data):
        self.sent_data.append(assets_data)
        if self.error:
            raise self.error

    def save_assets_state(
        self, integration_id, syncer_instance_id, assets_to_state: List[AssetToState]
    ):
        self.assets_state.append(assets_to_state)

    def update_last_cursor(self, integration_id, syncer_instance_id, state, data):
        self.last_cursor = data
        self.last_state = state

    def get_last_cursor_and_state(self) -> Tuple[int, str]:
        return self.last_cursor, self.last_state

    def get_last_cursor(self, integration_id, syncer_instance_id) -> int:
        return self.last_cursor

    def get_active_assets(self, integration_id, syncer_instance_id) -> List[dict]:
        assets_to_state = []
        if self.active_runs:
            for asset_to_state_dict in self.active_runs:
                try:
                    assets_to_state.append(AssetToState.from_dict(asset_to_state_dict))
                except Exception:
                    continue
            return assets_to_state

    def set_error(self, error):
        self.error = error

    def set_active_runs(self, active_runs):
        self.active_runs = active_runs


@pytest.fixture
def mock_tracking_service() -> MockTrackingService:  # type: ignore
    yield MockTrackingService("integration", "12345")
