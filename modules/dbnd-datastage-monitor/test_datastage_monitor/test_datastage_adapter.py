# © Copyright Databand.ai, an IBM Company 2022
import json

import pytest

from dbnd_datastage_monitor.adapter.datastage_adapter import DataStageAdapter
from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.datastage_client.datastage_assets_client import (
    ConcurrentRunsGetter,
)
from freezegun import freeze_time

from airflow_monitor.shared.adapter.adapter import Assets, AssetState, AssetToState
from dbnd import relative_path
from dbnd._core.utils.uid_utils import get_uuid

from .test_datastage_assets_client import init_mock_client


@pytest.fixture
def mock_datastage_config() -> DataStageServerConfig:
    yield DataStageServerConfig(
        uid=get_uuid(),
        source_name="test_syncer",
        source_type="integration",
        tracking_source_uid=get_uuid(),
        sync_interval=10,
        number_of_fetching_threads=2,
        project_ids=["1", "2"],
        fetching_interval_in_minutes=30,
    )


@pytest.fixture
def datastage_adapter(mock_datastage_config: DataStageServerConfig) -> DataStageAdapter:
    yield DataStageAdapter(
        datastage_assets_client=ConcurrentRunsGetter(client=init_mock_client()),
        config=mock_datastage_config,
    )


class TestDataStageAdapter:
    @freeze_time("2022-11-16T15:30:11Z")
    def test_datastage_adapter_init_cursor(self, datastage_adapter: DataStageAdapter):
        assert datastage_adapter.init_cursor() == "2022-11-16T15:30:11Z"

    @freeze_time("2022-07-28T13:25:19Z")
    def test_datastage_adapter_init_assets_for_cursor(
        self, datastage_adapter: DataStageAdapter
    ):
        for assets, last_cursor in datastage_adapter.init_assets_for_cursor(
            cursor=datastage_adapter.init_cursor(), batch_size=100
        ):
            expected_data = json.load(
                open(relative_path(__file__, "mocks/fetcher_response.json"))
            )
            assets.data is None
            assets.assets_to_state = [
                AssetToState(
                    asset_id=expected_data["runs"][0]["run_link"], state=AssetState.INIT
                )
            ]
            assert last_cursor == "2022-07-28T13:25:19Z"

    @freeze_time("2023-02-13T13:53:20Z")
    def test_datastage_adapter_init_assets_for_cursor_out_of_range(
        self, datastage_adapter: DataStageAdapter
    ):
        for assets, last_cursor in datastage_adapter.init_assets_for_cursor(
            cursor=datastage_adapter.init_cursor(), batch_size=100
        ):
            assets.data is None
            assert assets.assets_to_state == []
            assert last_cursor == "2023-02-13T13:53:20Z"

    def test_datastage_adapter_get_assets_data(
        self, datastage_adapter: DataStageAdapter
    ):
        expected_data = json.load(
            open(relative_path(__file__, "mocks/fetcher_response.json"))
        )
        assets = Assets(
            data=None,
            assets_to_state=[
                AssetToState(
                    asset_id=expected_data["runs"][0]["run_link"], state=AssetState.INIT
                )
            ],
        )
        expected_assets = datastage_adapter.get_assets_data(assets=assets)
        assert expected_data == expected_assets.data
        assert expected_assets.assets_to_state == [
            AssetToState(
                asset_id=expected_data["runs"][0]["run_link"], state=AssetState.ACTIVE
            )
        ]
