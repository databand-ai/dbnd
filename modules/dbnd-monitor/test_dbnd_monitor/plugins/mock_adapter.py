# © Copyright Databand.ai, an IBM Company 2022
from typing import Optional, Tuple

import pytest

from attr import evolve

from dbnd_monitor.adapter import Assets, AssetState, AssetToState, MonitorAdapter


class MockAdapter(MonitorAdapter):
    def __init__(self):
        super().__init__()
        self.cursor: int = 0
        self.error: Exception = None

    def set_error(self, error: Exception) -> None:
        self.error = error

    def get_new_assets_for_cursor(
        self, cursor: int, active_assets: Optional[Assets] = None
    ) -> Tuple[Assets, int]:
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


@pytest.fixture
def mock_adapter() -> MockAdapter:  # type: ignore
    yield MockAdapter()
