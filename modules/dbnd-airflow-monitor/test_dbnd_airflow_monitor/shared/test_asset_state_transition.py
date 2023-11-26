# © Copyright Databand.ai, an IBM Company 2022

import pytest

from airflow_monitor.shared.adapter.adapter import (
    AssetState,
    AssetToState,
    update_asset_retry_state,
    update_assets_retry_state,
)


class TestTransitionState:
    @pytest.mark.parametrize(
        "asset_to_state, expected_asset_to_state",
        [
            (
                AssetToState("asset1", state=AssetState.ACTIVE, retry_count=0),
                AssetToState("asset1", state=AssetState.ACTIVE, retry_count=0),
            ),
            (
                AssetToState("asset1", state=AssetState.ACTIVE, retry_count=3),
                AssetToState("asset1", state=AssetState.ACTIVE, retry_count=0),
            ),
            (
                AssetToState("asset2", state=AssetState.FAILED_REQUEST, retry_count=0),
                AssetToState("asset2", state=AssetState.FAILED_REQUEST, retry_count=1),
            ),
            (
                AssetToState("asset3", state=AssetState.FAILED_REQUEST, retry_count=4),
                AssetToState("asset3", state=AssetState.FAILED_REQUEST, retry_count=5),
            ),
            (
                AssetToState("asset4", state=AssetState.FAILED_REQUEST, retry_count=5),
                AssetToState("asset4", state=AssetState.FAILED_REQUEST, retry_count=6),
            ),
            (
                AssetToState("asset5", state=AssetState.FAILED_REQUEST, retry_count=6),
                AssetToState("asset5", state=AssetState.MAX_RETRY, retry_count=6),
            ),
            (
                AssetToState("asset6", state=AssetState.FAILED_REQUEST, retry_count=7),
                AssetToState("asset6", state=AssetState.MAX_RETRY, retry_count=7),
            ),
            (
                AssetToState("id", state=AssetState.MAX_RETRY, retry_count=2),
                AssetToState("id", state=AssetState.MAX_RETRY, retry_count=2),
            ),
            (
                AssetToState("id", state=AssetState.INIT, retry_count=-1),
                AssetToState("id", state=AssetState.INIT, retry_count=-1),
            ),
        ],
    )
    def test_update_asset_retry_state(self, asset_to_state, expected_asset_to_state):
        assert update_asset_retry_state(asset_to_state, 5) == expected_asset_to_state

    def test_update_assets_retry_state(self):
        assets = [
            AssetToState("id", state=AssetState.ACTIVE, retry_count=3),
            AssetToState("id", state=AssetState.INIT, retry_count=1),
            AssetToState("id", state=AssetState.FAILED_REQUEST, retry_count=4),
            AssetToState("id", state=AssetState.MAX_RETRY, retry_count=2),
            AssetToState("id", state=AssetState.FAILED_REQUEST, retry_count=2),
        ]
        expected = [
            AssetToState("id", state=AssetState.ACTIVE, retry_count=0),
            AssetToState("id", state=AssetState.INIT, retry_count=1),
            AssetToState("id", state=AssetState.MAX_RETRY, retry_count=4),
            AssetToState("id", state=AssetState.MAX_RETRY, retry_count=2),
            AssetToState("id", state=AssetState.FAILED_REQUEST, retry_count=3),
        ]
        assert update_assets_retry_state(assets, 3) == expected
