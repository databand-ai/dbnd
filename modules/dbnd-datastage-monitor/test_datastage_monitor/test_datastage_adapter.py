# © Copyright Databand.ai, an IBM Company 2022
import json

import pytest

from dbnd_datastage_monitor.adapter.datastage_adapter import DataStageAdapter
from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.datastage_client.datastage_assets_client import (
    ConcurrentRunsGetter,
)
from freezegun import freeze_time

from dbnd import relative_path

from .test_datastage_assets_client import init_mock_client


@pytest.fixture
def mock_datastage_config() -> DataStageServerConfig:
    yield DataStageServerConfig(
        source_name="test_syncer",
        source_type="integration",
        tracking_source_uid="12345",
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
    def test_datastage_adapter_get_last_cursor(
        self, datastage_adapter: DataStageAdapter
    ):
        assert datastage_adapter.get_last_cursor() == "2022-11-16T15:30:11Z"

    @freeze_time("2022-07-20T23:01:08Z")
    def test_datastage_adapter_get_data(self, datastage_adapter: DataStageAdapter):
        data, failed, next_page = datastage_adapter.get_data(
            cursor=datastage_adapter.get_last_cursor(), batch_size=100, next_page=None
        )
        expected_data = json.load(
            open(relative_path(__file__, "mocks/fetcher_response.json"))
        )
        assert expected_data == data
        assert datastage_adapter.get_last_cursor() == "2022-07-20T23:01:08Z"

    @freeze_time("2023-02-13T13:53:20Z")
    def test_datastage_adapter_get_data_out_of_range(
        self, datastage_adapter: DataStageAdapter
    ):
        data, failed, next_page = datastage_adapter.get_data(
            cursor=datastage_adapter.get_last_cursor(), batch_size=100, next_page=None
        )
        assert data is None
        assert datastage_adapter.get_last_cursor() == "2023-02-13T13:53:20Z"
