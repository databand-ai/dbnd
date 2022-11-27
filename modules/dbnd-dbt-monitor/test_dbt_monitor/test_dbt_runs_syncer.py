# © Copyright Databand.ai, an IBM Company 2022

from unittest.mock import MagicMock, patch

import attr
import pytest

from dbnd_dbt_monitor.data.dbt_config_data import DbtServerConfig
from dbnd_dbt_monitor.fetcher.dbt_cloud_data_fetcher import DbtCloudDataFetcher
from dbnd_dbt_monitor.syncer.dbt_runs_syncer import DbtRunsSyncer
from more_itertools import one

from airflow_monitor.shared.base_tracking_service import BaseDbndTrackingService


RUNNING_STATE = "running"
FINISHED_STATE = "success"


@attr.s(auto_attribs=True)
class MockDbtRun:
    id: int
    state: str


class MockDbtFetcher(DbtCloudDataFetcher):
    def __init__(self):
        dbt_cloud_api_client = MagicMock()
        super(MockDbtFetcher, self).__init__(dbt_cloud_api_client, 10)
        self.dbt_runs = []

    def get_run_ids_to_sync_from_dbt(self, dbnd_last_run_id):
        return [
            dbt_run.id for dbt_run in self.dbt_runs if dbt_run.id > dbnd_last_run_id
        ]

    def get_last_seen_run_id(self):
        all_ids = [dbt_run.id for dbt_run in self.dbt_runs]
        return max(all_ids) if all_ids else None

    def get_full_dbt_runs(self, dbt_run_ids):
        return [dbt_run for dbt_run in self.dbt_runs if dbt_run.id in dbt_run_ids]


class MockDbtTrackingService(BaseDbndTrackingService):
    def __init__(self, tracking_source_uid=None):
        super(MockDbtTrackingService, self).__init__(
            monitor_type="airflow",
            tracking_source_uid=tracking_source_uid,
            server_monitor_config=DbtServerConfig,
        )
        self.dbt_runs = []
        self.last_seen_run_id = None

    def update_last_seen_values(self, last_seen_run_id):
        self.last_seen_run_id = last_seen_run_id

    def init_dbt_runs(self, dbt_runs_full_data):
        new_runs = [
            MockDbtRun(id=run.id, state=run.state) for run in dbt_runs_full_data
        ]
        self.dbt_runs.extend(new_runs)
        all_ids = [dbt_run.id for dbt_run in self.dbt_runs]
        if all_ids:
            max_id = max(all_ids)
            if max_id > self.last_seen_run_id:
                self.last_seen_run_id = max_id

    def update_dbt_runs(self, dbt_runs_full_data):
        for dbt_run in dbt_runs_full_data:
            existing = one(d for d in self.dbt_runs if d.id == dbt_run.id)
            existing.state = dbt_run.state

    def get_running_dbt_run_ids(self):
        return [
            dbt_run.id for dbt_run in self.dbt_runs if dbt_run.state == RUNNING_STATE
        ]

    def get_last_seen_run_id(self):
        return self.last_seen_run_id


@pytest.fixture
def mock_dbt_tracking_service() -> MockDbtTrackingService:
    yield MockDbtTrackingService()


@pytest.fixture
def mock_dbt_fetcher() -> MockDbtFetcher:
    yield MockDbtFetcher()


@pytest.fixture
def dbt_runtime_syncer(mock_dbt_fetcher, mock_dbt_tracking_service):
    syncer = DbtRunsSyncer(
        config=DbtServerConfig(
            source_name="test",
            source_type="airflow",
            tracking_source_uid=mock_dbt_tracking_service.tracking_source_uid,
        ),
        tracking_service=mock_dbt_tracking_service,
        data_fetcher=mock_dbt_fetcher,
    )
    with patch.object(
        syncer, "tracking_service", wraps=syncer.tracking_service
    ), patch.object(syncer, "data_fetcher", wraps=syncer.data_fetcher):
        yield syncer


def expect_changes(
    runtime_syncer,
    init=0,
    update=0,
    is_dbnd_empty=False,
    is_dbt_empty=False,
    reset=True,
):
    patched_data_fetcher = runtime_syncer.data_fetcher
    patched_tracking_service = runtime_syncer.tracking_service

    if is_dbnd_empty:
        if is_dbt_empty:
            assert patched_tracking_service.update_last_seen_values.call_count == 0
            assert patched_tracking_service.get_running_dbt_run_ids.call_count == 0
            assert patched_data_fetcher.get_run_ids_to_sync_from_dbt.call_count == 0
        else:
            assert patched_tracking_service.update_last_seen_values.call_count == 1
            assert patched_tracking_service.get_running_dbt_run_ids.call_count == 0
            assert patched_data_fetcher.get_run_ids_to_sync_from_dbt.call_count == 0
    else:
        assert patched_tracking_service.get_running_dbt_run_ids.call_count == 1
        assert patched_data_fetcher.get_run_ids_to_sync_from_dbt.call_count == 1
        if init == 0:
            assert patched_tracking_service.update_last_seen_values.call_count == 0
        else:
            assert patched_tracking_service.update_last_seen_values.call_count == 1

    assert patched_data_fetcher.get_full_dbt_runs.call_count == init + update
    assert patched_tracking_service.init_dbt_runs.call_count == init
    assert patched_tracking_service.update_dbt_runs.call_count == update

    if reset:
        patched_data_fetcher.reset_mock()
        patched_tracking_service.reset_mock()


class TestDbtRunsSyncer:
    def test_sync_dbt_runs(self, dbt_runtime_syncer, mock_dbt_fetcher):
        # First sync, nothing should happen
        dbt_runtime_syncer.sync_once()
        expect_changes(
            dbt_runtime_syncer, init=0, update=0, is_dbnd_empty=True, is_dbt_empty=True
        )

        # There is a run in dbt, should set the last_seen_run_id and not sync anything
        old_dbt_run = MockDbtRun(id=1, state=FINISHED_STATE)
        mock_dbt_fetcher.dbt_runs.append(old_dbt_run)
        dbt_runtime_syncer.sync_once()
        expect_changes(dbt_runtime_syncer, init=0, update=0, is_dbnd_empty=True)

        # There is a new run, should sync it to dbnd
        new_dbt_run = MockDbtRun(id=2, state=RUNNING_STATE)
        mock_dbt_fetcher.dbt_runs.append(new_dbt_run)
        dbt_runtime_syncer.sync_once()
        expect_changes(dbt_runtime_syncer, init=1, update=0)

        # The run is in state running in dbnd, so should try to update it
        dbt_runtime_syncer.sync_once()
        expect_changes(dbt_runtime_syncer, init=0, update=1)

        # Still running in dbnd, should update to the new state (finished)
        new_dbt_run.state = FINISHED_STATE
        dbt_runtime_syncer.sync_once()
        expect_changes(dbt_runtime_syncer, init=0, update=1)

        # No new runs in dbt or running runs in dbnd, should do nothing
        dbt_runtime_syncer.sync_once()
        expect_changes(dbt_runtime_syncer, init=0, update=0)
