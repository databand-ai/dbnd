# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd_dbt_monitor.adapter.dbt_adapter import DbtAdapter

from airflow_monitor.shared.adapter.adapter import AssetState, AssetToState

from .test_dbt_runs_syncer import FINISHED_STATE, MockDbtFetcher, MockDbtRun


def generate_mock_dbt_runs(num):
    for i in range(num):
        yield MockDbtRun(id=i, state=FINISHED_STATE)


@pytest.fixture
def mock_dbt_data_fetcher() -> MockDbtFetcher:
    mock_data_fetcher = MockDbtFetcher()
    for mock_dbt_run in generate_mock_dbt_runs(10):
        mock_data_fetcher.dbt_runs.append(mock_dbt_run)
    yield mock_data_fetcher


@pytest.fixture
def dbt_adapter(mock_dbt_data_fetcher) -> DbtAdapter:
    yield DbtAdapter(data_fetcher=mock_dbt_data_fetcher)


class TestDbtAdapter:
    def test_dbt_adapter_init_cursor(self, dbt_adapter: DbtAdapter):
        assert dbt_adapter.init_cursor() == 9

    def test_dbt_adapter_init_assets_for_cursor(self, dbt_adapter: DbtAdapter):
        for result_assets, next_cursor in dbt_adapter.init_assets_for_cursor(
            cursor=5, batch_size=200
        ):
            assert result_assets.assets_to_state == [
                AssetToState(asset_id="6", state=AssetState.INIT, retry_count=0),
                AssetToState(asset_id="7", state=AssetState.INIT, retry_count=0),
                AssetToState(asset_id="8", state=AssetState.INIT, retry_count=0),
                AssetToState(asset_id="9", state=AssetState.INIT, retry_count=0),
            ]
            assert next_cursor == 9

    def test_dbt_adapter_init_assets_for_last_cursor(self, dbt_adapter: DbtAdapter):
        for result_assets, next_cursor in dbt_adapter.init_assets_for_cursor(
            cursor=9, batch_size=200
        ):
            assert result_assets.assets_to_state == []
            assert next_cursor == 9
