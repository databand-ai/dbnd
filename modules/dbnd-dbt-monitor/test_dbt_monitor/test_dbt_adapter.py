# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd_dbt_monitor.adapter.dbt_adapter import DbtAdapter

from .test_dbt_runs_syncer import FINISHED_STATE, MockDbtFetcher, MockDbtRun


@pytest.fixture
def mock_dbt_data_fetcher() -> MockDbtFetcher:
    old_dbt_run = MockDbtRun(id=1, state=FINISHED_STATE)
    mock_data_fetcher = MockDbtFetcher()
    mock_data_fetcher.dbt_runs.append(old_dbt_run)
    yield mock_data_fetcher


@pytest.fixture
def dbt_adapter(mock_dbt_data_fetcher) -> DbtAdapter:
    yield DbtAdapter(data_fetcher=mock_dbt_data_fetcher)


class TestDbtAdapter:
    def test_dbt_adapter_init_cursor(self, dbt_adapter: DbtAdapter):
        assert dbt_adapter.init_cursor() == 1
