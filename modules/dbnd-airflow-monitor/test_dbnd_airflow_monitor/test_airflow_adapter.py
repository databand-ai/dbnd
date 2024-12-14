# © Copyright Databand.ai, an IBM Company 2024
from dataclasses import dataclass
from typing import List
from unittest.mock import patch
from uuid import uuid4

import pytest

from airflow_monitor.adapter.airflow_adapter import AirflowAdapter, update_assets_states
from airflow_monitor.common.airflow_data import (
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
)
from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from dbnd_monitor.adapter.adapter import Assets, AssetState, AssetToState
from test_dbnd_airflow_monitor.airflow_utils import can_be_dead

from .mock_airflow_data_fetcher import MockDagRun, MockDataFetcher


@dataclass
class DagRuns:
    dag_run_1 = MockDagRun(id=10, dag_id="dag1", state="running", is_paused=False)
    dag_run_2 = MockDagRun(id=11, dag_id="dag2", state="running", is_paused=False)
    dag_run_3 = MockDagRun(id=12, dag_id="dag3", state="running", is_paused=False)
    dag_run_4 = MockDagRun(id=13, dag_id="dag4", state="running", is_paused=False)

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
        "sync_interval": 0,
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
    assert no_dag_runs_cursor.last_seen_dag_run_id is None

    mock_data_fetcher.dag_runs = mock_dag_runs.as_list()
    new_dag_runs_cursor = airflow_adapter.init_cursor()
    assert new_dag_runs_cursor.last_seen_dag_run_id == mock_dag_runs.dag_run_4.id


@pytest.mark.parametrize(
    [
        "last_seen_dag_run_id",
        "airflow_dag_runs_to_sync",
        "expected_new_last_seen_dag_run_id",
        "expected_dag_run_ids",
    ],
    [
        (None, [], None, []),
        (13, [10, 11, 12, 13], 13, []),
        (None, [10, 11, 12, 13], 13, [10, 11, 12, 13]),
        (11, [10, 11, 12, 13], 13, [12, 13]),
    ],
)
def test_get_new_assets_for_cursor(
    airflow_adapter,
    mock_tracking_service,
    mock_data_fetcher,
    mock_dag_runs,
    last_seen_dag_run_id,
    airflow_dag_runs_to_sync,
    expected_new_last_seen_dag_run_id,
    expected_dag_run_ids,
):
    dag_runs_dict = mock_dag_runs.as_dict()
    mock_data_fetcher.dag_runs = [dag_runs_dict[id] for id in airflow_dag_runs_to_sync]

    cursor = LastSeenValues(last_seen_dag_run_id)
    new_assets, new_cursor = airflow_adapter.get_new_assets_for_cursor(cursor)
    new_dag_run_ids = [int(asset.asset_id) for asset in new_assets.assets_to_state]

    assert new_cursor.last_seen_dag_run_id == expected_new_last_seen_dag_run_id
    assert new_dag_run_ids == expected_dag_run_ids

    for asset in new_assets.assets_to_state:
        assert asset.state == AssetState.INIT


def test_get_assets_data_no_assets(airflow_adapter, mock_dag_runs, mock_data_fetcher):
    empty_assets = Assets()
    assets = airflow_adapter.get_assets_data(empty_assets)

    assert not assets.data
    assert not assets.assets_to_state


def test_get_assets_data_new_runs(airflow_adapter, mock_dag_runs, mock_data_fetcher):
    mock_data_fetcher.dag_runs = mock_dag_runs.as_list()

    cursor = LastSeenValues(None)
    new_runs_assets, _ = airflow_adapter.get_new_assets_for_cursor(cursor)
    assets = airflow_adapter.get_assets_data(new_runs_assets)

    assert "dags" in assets.data
    assert "dag_runs" in assets.data
    assert "task_instances" in assets.data
    assert "airflow_instance_uid" in assets.data


def test_get_assets_data_active_runs(
    airflow_adapter,
    airflow_integration_config,
    mock_dag_runs,
    mock_tracking_service,
    mock_data_fetcher,
):
    mock_tracking_service.dag_runs = mock_dag_runs.as_list()
    mock_data_fetcher.dag_runs = mock_dag_runs.as_list()

    active_runs_assets_to_state = mock_tracking_service.get_active_assets(
        str(airflow_integration_config.uid),
        str(airflow_integration_config.tracking_source_uid),
    )
    active_runs_assets = Assets(assets_to_state=active_runs_assets_to_state)
    assets = airflow_adapter.get_assets_data(active_runs_assets)

    assert "dags" not in assets.data
    assert "dag_runs" in assets.data
    assert "task_instances" in assets.data
    assert "airflow_instance_uid" in assets.data


def test_update_assets_states(airflow_adapter, mock_dag_runs):
    assets_to_state = [AssetToState(asset_id=str(id)) for id in mock_dag_runs.as_dict()]
    assets_to_state.append(AssetToState(asset_id=str(14)))

    mock_dag_runs.dag_run_2.state = "queued"
    mock_dag_runs.dag_run_3.state = "success"
    mock_dag_runs.dag_run_4.state = "failed"

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
