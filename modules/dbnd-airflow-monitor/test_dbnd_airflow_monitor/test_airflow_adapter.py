# © Copyright Databand.ai, an IBM Company 2024
from dataclasses import dataclass
from typing import List
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from airflow_monitor.adapter.airflow_adapter import (
    AirflowAdapter,
    merge_dag_runs_full_and_state_data,
    split_assets_by_active_state,
    update_assets_states,
)
from airflow_monitor.common.airflow_data import DagRunsFullData, DagRunsStateData
from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from dbnd_airflow.export_plugin.models import DagRunState
from dbnd_monitor.adapter.adapter import Assets, AssetState, AssetToState
from test_dbnd_airflow_monitor.airflow_utils import can_be_dead
from test_dbnd_airflow_monitor.mocks.mock_airflow_data_fetcher import (
    MockDagRun,
    MockDataFetcher,
)


@dataclass
class DagRuns:
    dag_run_1 = MockDagRun(
        id=10, dag_id="dag1", state=DagRunState.RUNNING, is_paused=False
    )
    dag_run_2 = MockDagRun(
        id=11, dag_id="dag2", state=DagRunState.RUNNING, is_paused=False
    )
    dag_run_3 = MockDagRun(
        id=12, dag_id="dag3", state=DagRunState.RUNNING, is_paused=False
    )
    dag_run_4 = MockDagRun(
        id=13, dag_id="dag4", state=DagRunState.RUNNING, is_paused=False
    )

    def as_list(self):
        return [self.dag_run_1, self.dag_run_2, self.dag_run_3, self.dag_run_4]

    def as_dict(self):
        return {dag_run.id: dag_run for dag_run in self.as_list()}


@pytest.fixture
def mock_dag_runs() -> DagRuns:
    return DagRuns()


@pytest.fixture
def airflow_integration_config() -> AirflowIntegrationConfig:
    config = {
        "uid": uuid4(),
        "source_type": "airflow",
        "source_name": "mock_server_1",
        "tracking_source_uid": uuid4(),
    }
    return AirflowIntegrationConfig(**config)


class ActualMockDataFetcher(MockDataFetcher):
    @can_be_dead
    def get_full_dag_runs(
        self, dag_run_ids: List[int], include_sources: bool
    ) -> DagRunsFullData:
        data = super().get_full_dag_runs(dag_run_ids, include_sources)
        data.dag_runs = [dag_run.as_dict() for dag_run in data.dag_runs]
        return data

    @can_be_dead
    def get_dag_runs_state_data(self, dag_run_ids: List[int]) -> DagRunsStateData:
        data = super().get_dag_runs_state_data(dag_run_ids)
        data.dag_runs = [dag_run.as_dict() for dag_run in data.dag_runs]
        return data


@pytest.fixture
def mock_data_fetcher() -> DbFetcher:
    return ActualMockDataFetcher()


@pytest.fixture
def airflow_adapter(airflow_integration_config, mock_data_fetcher) -> AirflowAdapter:
    with patch(
        "airflow_monitor.adapter.airflow_adapter.get_or_create_airflow_instance_uid"
    ), patch(
        "airflow_monitor.adapter.airflow_adapter.DbFetcher",
        return_value=mock_data_fetcher,
    ):
        return AirflowAdapter(airflow_integration_config)


def test_init_cursor(airflow_adapter, mock_data_fetcher, mock_dag_runs):
    no_dag_runs_cursor = airflow_adapter.init_cursor()
    assert no_dag_runs_cursor is None

    mock_data_fetcher.dag_runs = mock_dag_runs.as_list()
    new_dag_runs_cursor = airflow_adapter.init_cursor()
    assert new_dag_runs_cursor == mock_dag_runs.dag_run_4.id


@pytest.mark.parametrize(
    [
        "cursor",
        "airflow_active_dag_runs",
        "dbnd_active_dag_runs",
        "expected_new_cursor",
        "expected_dag_run_ids",
    ],
    [
        (None, [], [], None, []),
        (None, [10, 11, 12, 13], [], 13, [10, 11, 12, 13]),
        (11, [10, 11, 12, 13], [10, 11], 13, [12, 13]),
        (13, [12, 13], [10, 11, 12, 13], 13, []),
    ],
)
def test_get_new_assets_for_cursor(
    airflow_adapter,
    mock_tracking_service,
    mock_data_fetcher,
    mock_dag_runs,
    cursor,
    airflow_active_dag_runs,
    dbnd_active_dag_runs,
    expected_new_cursor,
    expected_dag_run_ids,
):
    dag_runs_dict = mock_dag_runs.as_dict()
    mock_data_fetcher.dag_runs = [dag_runs_dict[id] for id in airflow_active_dag_runs]

    active_assets = Assets(
        assets_to_state=[AssetToState(str(id)) for id in dbnd_active_dag_runs]
    )

    new_assets, new_cursor = airflow_adapter.get_new_assets_for_cursor(
        cursor, active_assets
    )
    new_dag_run_ids = [int(asset.asset_id) for asset in new_assets.assets_to_state]
    new_dag_run_ids = sorted(new_dag_run_ids)

    assert new_cursor == expected_new_cursor
    assert new_dag_run_ids == expected_dag_run_ids

    for asset in new_assets.assets_to_state:
        assert asset.state == AssetState.INIT


def test_get_assets_data_no_assets(airflow_adapter, mock_dag_runs, mock_data_fetcher):
    empty_assets = Assets()
    assets = airflow_adapter.get_assets_data(empty_assets)

    assert not assets.data
    assert not assets.assets_to_state


@pytest.mark.parametrize(
    ["assets_to_state_dict", "expected_full_data_assets_count"],
    [
        ({10: AssetState.INIT, 11: AssetState.FAILED_REQUEST}, 2),
        ({12: AssetState.ACTIVE}, 0),
        ({12: AssetState.ACTIVE, 13: AssetState.INIT}, 1),
    ],
)
def test_get_assets_data_no_empty_assets(
    airflow_adapter,
    mock_dag_runs,
    mock_data_fetcher,
    assets_to_state_dict,
    expected_full_data_assets_count,
):
    airflow_adapter.get_assets_full_data = MagicMock(
        wraps=airflow_adapter.get_assets_full_data
    )
    airflow_adapter.get_assets_state_data = MagicMock(
        wraps=airflow_adapter.get_assets_state_data
    )

    mock_data_fetcher.dag_runs = mock_dag_runs.as_list()
    assets_to_state = [
        AssetToState(str(id), state=state) for id, state in assets_to_state_dict.items()
    ]
    assets_to_get_data = Assets(assets_to_state=assets_to_state)

    assets = airflow_adapter.get_assets_data(assets_to_get_data)

    full_data_assets_count = len(airflow_adapter.get_assets_full_data.call_args[0][0])
    assert full_data_assets_count == expected_full_data_assets_count

    state_data_assets_count = len(airflow_adapter.get_assets_state_data.call_args[0][0])
    expected_state_data_assets_count = (
        len(assets_to_state) - expected_full_data_assets_count
    )
    assert state_data_assets_count == expected_state_data_assets_count

    should_have_dags = expected_full_data_assets_count > 0

    assert ("dags" in assets.data) == should_have_dags
    assert "dag_runs" in assets.data
    assert "task_instances" in assets.data
    assert "airflow_instance_uid" in assets.data


def test_update_assets_states(airflow_adapter, mock_dag_runs):
    assets_to_state = [AssetToState(asset_id=str(id)) for id in mock_dag_runs.as_dict()]
    assets_to_state.append(AssetToState(asset_id=str(14)))

    mock_dag_runs.dag_run_2.state = DagRunState.QUEUED
    mock_dag_runs.dag_run_3.state = DagRunState.SUCCESS
    mock_dag_runs.dag_run_4.state = DagRunState.FAILED

    dag_runs = list(map(lambda dag_run: dag_run.as_dict(), mock_dag_runs.as_list()))

    updated_assets_to_state = update_assets_states(assets_to_state, dag_runs)

    expected_states = [
        AssetState.ACTIVE,
        AssetState.ACTIVE,
        AssetState.FINISHED,
        AssetState.FINISHED,
        AssetState.FAILED_REQUEST,
    ]
    actual_states = list(map(lambda asset: asset.state, updated_assets_to_state))

    assert actual_states == expected_states


def test_split_assets_by_active_state():
    expected_active = [AssetToState(asset_id="1", state=AssetState.ACTIVE)]
    expected_others = [
        AssetToState(asset_id="2", state=AssetState.INIT),
        AssetToState(asset_id="3", state=AssetState.FAILED_REQUEST),
        AssetToState(asset_id="4", state=AssetState.FINISHED),
    ]

    all = expected_others + expected_active

    active, others = split_assets_by_active_state(all)

    assert active == expected_active
    assert others == expected_others


@pytest.mark.parametrize("dags", [[], [1, 2]])
def test_merge_dag_runs_full_and_state_data(mock_dag_runs, dags):
    dag_runs = mock_dag_runs.as_list()
    task_instances = [1, 2, 3, 4]
    full_data = DagRunsFullData(
        dags=dags, dag_runs=dag_runs[:2], task_instances=task_instances[:2]
    )
    state_data = DagRunsStateData(
        dag_runs=dag_runs[2:], task_instances=task_instances[2:]
    )

    data = merge_dag_runs_full_and_state_data(full_data, state_data)

    expected_data_class = DagRunsFullData if dags else DagRunsStateData
    assert isinstance(data, expected_data_class)

    assert not dags or data.dags == dags
    assert data.dag_runs == dag_runs
    assert data.task_instances == task_instances
