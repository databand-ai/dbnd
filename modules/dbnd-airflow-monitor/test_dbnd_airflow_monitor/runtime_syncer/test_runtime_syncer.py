# © Copyright Databand.ai, an IBM Company 2022
import random
import time

import pytest

from mock import Mock, patch

from airflow_monitor.common.config_data import AirflowIntegrationConfig
from airflow_monitor.syncer.runtime_syncer import AirflowRuntimeSyncer
from dbnd._core.utils.uid_utils import get_uuid

from ..mock_airflow_data_fetcher import MockDagRun


@pytest.fixture
def runtime_syncer(mock_data_fetcher, mock_tracking_service, mock_reporting_service):
    syncer = AirflowRuntimeSyncer(
        config=AirflowIntegrationConfig(
            uid=get_uuid(),
            source_name="test",
            source_type="airflow",
            tracking_source_uid=mock_tracking_service.tracking_source_uid,
        ),
        tracking_service=mock_tracking_service,
        data_fetcher=mock_data_fetcher,
        reporting_service=mock_reporting_service,
    )
    with patch.object(
        syncer, "tracking_service", wraps=syncer.tracking_service
    ), patch.object(syncer, "data_fetcher", wraps=syncer.data_fetcher), patch.object(
        syncer, "reporting_service", wraps=syncer.reporting_service
    ):
        yield syncer


def expect_changes(
    runtime_syncer, init=0, update=0, is_dbnd_empty=False, reset=True, case=None
):
    patched_data_fetcher = runtime_syncer.data_fetcher
    patched_tracking_service = runtime_syncer.tracking_service
    prefix = f"case: {case}: " if case else ""

    assert (
        patched_data_fetcher.get_airflow_dagruns_to_sync.call_count == 1
    ), f"{prefix} get_airflow_dagruns_to_sync.call_count doesn't match"

    if is_dbnd_empty:
        assert patched_tracking_service.update_last_seen_values.call_count == 1
        assert (
            patched_tracking_service.get_active_dag_runs.call_count == 2
        ), f"{prefix} get_dbnd_dags_to_sync.call_count doesn't match"
    else:
        assert (
            patched_tracking_service.get_active_dag_runs.call_count == 1
        ), f"{prefix} get_dbnd_dags_to_sync.call_count doesn't match"

    assert (
        patched_data_fetcher.get_full_dag_runs.call_count == init
    ), f"{prefix} get_full_dag_runs.call_count doesn't match"
    assert (
        patched_tracking_service.init_dagruns.call_count == init
    ), f"{prefix} init_dagruns.call_count doesn't match"

    assert (
        patched_data_fetcher.get_dag_runs_state_data.call_count == update
    ), f"{prefix} get_dag_runs_state_data.call_count doesn't match"
    # if no updates happened - then we should see "empty" update for heartbeat
    assert patched_tracking_service.update_dagruns.call_count == (
        update or 1
    ), f"{prefix} update_dagruns.call_count doesn't match"

    if reset:
        patched_data_fetcher.reset_mock()
        patched_tracking_service.reset_mock()


class TestRuntimeSyncer:
    def test_01_simple_calls_flow(self, runtime_syncer, mock_data_fetcher):
        # both dbnd and airflow are empty
        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=0,
            update=0,
            is_dbnd_empty=True,
            case="both dbnd and airflow are empty",
        )

        # in airflow one dagrun started to run => should do one init
        airflow_dag_run = MockDagRun(id=10, state="RUNNING", is_paused=False)
        mock_data_fetcher.dag_runs.append(airflow_dag_run)

        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=1,
            update=0,
            case="in airflow one dagrun started to run => should do one init",
        )

        # finished in airflow, running in dbnd
        airflow_dag_run.state = "FINISHED"
        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=0,
            update=1,
            case="finished in airflow, running in dbnd",
        )

        # finished both in dbnd and airflow => nothing
        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=0,
            update=0,
            case="finished both in dbnd and airflow => nothing",
        )

    def test_02_init_dagruns_in_bulk(
        self, runtime_syncer, mock_data_fetcher, mock_tracking_service
    ):
        mock_data_fetcher.dag_runs = sorted(
            [MockDagRun(id=i, dag_id=f"dag{i}") for i in range(11)],
            key=lambda _: random.random(),
        )
        runtime_syncer.config.dag_run_bulk_size = 3
        runtime_syncer.sync_once()

        # we should have 4 init calls - 3 iterations of 3 dag runs and 1 iteration of 2
        expect_changes(
            runtime_syncer,
            init=4,
            update=0,
            reset=False,
            is_dbnd_empty=True,
            case="should have 4 init calls - 3 iterations of 3 dag runs and 1 iteration of 2",
        )

        # noinspection PyTypeChecker
        mock_init_dagruns = runtime_syncer.tracking_service.init_dagruns  # type: Mock

        # called for dagruns: 0,1,2 ; 3,4,5 ; 6,7,8 ; 9,10 (order inside bulk doesn't matter)
        for i in range(4):
            assert sorted(
                dr.id for dr in mock_init_dagruns.call_args_list[i].args[0].dag_runs
            ) == list(range(i * 3, min(i * 3 + 3, 11)))

    def test_03_init_dagruns_oneshot(
        self, runtime_syncer, mock_data_fetcher, mock_tracking_service
    ):
        mock_data_fetcher.dag_runs = sorted(
            [MockDagRun(id=i, dag_id=f"dag{i}") for i in range(11)],
            key=lambda _: random.random(),
        )
        runtime_syncer.config.dag_run_bulk_size = 0
        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer, init=1, update=0, is_dbnd_empty=True, reset=False
        )

        # noinspection PyTypeChecker
        mock_init_dagruns = runtime_syncer.tracking_service.init_dagruns  # type: Mock
        assert sorted(
            [dr.id for dr in mock_init_dagruns.call_args.args[0].dag_runs]
        ) == list(range(11))

    def test_04_update_dagruns_oneshot(
        self, runtime_syncer, mock_data_fetcher, mock_tracking_service
    ):
        mock_tracking_service.dag_runs = [
            MockDagRun(id=i, dag_id=f"dag{i}") for i in range(11)
        ]
        mock_data_fetcher.dag_runs = sorted(
            [MockDagRun(id=i, dag_id=f"dag{i}", state="FINISHED") for i in range(11)],
            key=lambda _: random.random(),
        )
        runtime_syncer.config.dag_run_bulk_size = 0
        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer, init=0, update=1, is_dbnd_empty=True, reset=False
        )

        # noinspection PyTypeChecker
        mock_update_dagruns = (
            runtime_syncer.tracking_service.update_dagruns
        )  # type: Mock
        assert sorted(
            [dr.id for dr in mock_update_dagruns.call_args.args[0].dag_runs]
        ) == list(range(11))

    def test_06_initial_state(
        self, runtime_syncer, mock_data_fetcher, mock_tracking_service
    ):
        assert mock_tracking_service.last_seen_dag_run_id is None

        runtime_syncer.sync_once()

        expect_changes(
            runtime_syncer, init=0, update=0, is_dbnd_empty=True, reset=False
        )
        assert mock_tracking_service.last_seen_dag_run_id == -1

    def test_07_initial_state_af_non_empty(
        self, runtime_syncer, mock_data_fetcher, mock_tracking_service
    ):
        assert mock_tracking_service.last_seen_dag_run_id is None

        mock_data_fetcher.dag_runs = sorted(
            [MockDagRun(id=i, dag_id=f"dag{i}", state="FINISHED") for i in range(11)],
            key=lambda _: random.random(),
        )
        runtime_syncer.sync_once()

        expect_changes(
            runtime_syncer, init=0, update=0, is_dbnd_empty=True, reset=False
        )
        assert mock_tracking_service.last_seen_dag_run_id == 10
        assert not mock_tracking_service.dag_runs

    def test_08_dag_ids(self, runtime_syncer, mock_data_fetcher, mock_tracking_service):
        runtime_syncer.config.dag_ids = "dag2"

        # both dbnd and airflow are empty
        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=0,
            update=0,
            is_dbnd_empty=True,
            case="both dbnd and airflow are empty",
        )

        # in airflow one dagrun started to run => should do one init
        airflow_dag_run1 = MockDagRun(
            id=10, dag_id="dag1", state="RUNNING", is_paused=False
        )
        airflow_dag_run2 = MockDagRun(
            id=11, dag_id="dag2", state="RUNNING", is_paused=False
        )
        mock_data_fetcher.dag_runs.append(airflow_dag_run1)
        mock_data_fetcher.dag_runs.append(airflow_dag_run2)

        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=1,
            update=0,
            case="in airflow one dagrun started to run => should do one init",
        )

        # finished in airflow, running in dbnd
        airflow_dag_run2.state = "FINISHED"
        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=0,
            update=1,
            case="finished in airflow, running in dbnd",
        )

        # finished both in dbnd and airflow => nothing
        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=0,
            update=0,
            case="finished both in dbnd and airflow => nothing",
        )

    def test_09_syncer_auto_restarted(self, runtime_syncer, mock_data_fetcher):
        assert runtime_syncer.last_success_sync is None
        assert runtime_syncer.last_sync_heartbeat is None
        for _ in range(3):
            runtime_syncer.sync_once()
        assert runtime_syncer.last_success_sync
        assert runtime_syncer.last_sync_heartbeat
        runtime_syncer.config.restart_after_not_synced_minutes = 2 / 60  # 2 seconds
        time.sleep(3)
        # make sure that even after 3 seconds sleep sync_once() will work and won't
        # restart (since it's sleep between iterations)
        for _ in range(3):
            runtime_syncer.sync_once()

        last_success_sync = runtime_syncer.last_success_sync
        last_sync_heartbeat = runtime_syncer.last_sync_heartbeat
        # now simulate that data_fetcher fails
        mock_data_fetcher.alive = False

        runtime_syncer.sync_once()
        assert last_sync_heartbeat != runtime_syncer.last_sync_heartbeat
        last_sync_heartbeat = runtime_syncer.last_sync_heartbeat
        # last_success_sync shouldn't change
        assert last_success_sync == runtime_syncer.last_success_sync
        time.sleep(3)
        # first iteration after sleep should still work
        runtime_syncer.sync_once()
        assert last_sync_heartbeat != runtime_syncer.last_sync_heartbeat
        assert last_success_sync == runtime_syncer.last_success_sync

        # next iteration should cause auto-restart
        with pytest.raises(SystemExit):
            runtime_syncer.sync_once()

    def test_10_excluded_dag_ids(
        self, runtime_syncer, mock_data_fetcher, mock_tracking_service
    ):
        runtime_syncer.config.excluded_dag_ids = "dag1"

        # both dbnd and airflow are empty
        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=0,
            update=0,
            is_dbnd_empty=True,
            case="both dbnd and airflow are empty",
        )

        # in airflow one dagrun started to run => should do one init
        airflow_dag_run1 = MockDagRun(
            id=10, dag_id="dag1", state="RUNNING", is_paused=False
        )
        airflow_dag_run2 = MockDagRun(
            id=11, dag_id="dag2", state="RUNNING", is_paused=False
        )
        mock_data_fetcher.dag_runs.append(airflow_dag_run1)
        mock_data_fetcher.dag_runs.append(airflow_dag_run2)

        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=1,
            update=0,
            case="in airflow one dagrun started to run => should do one init",
        )

        # finished in airflow, running in dbnd
        airflow_dag_run2.state = "FINISHED"
        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=0,
            update=1,
            case="finished in airflow, running in dbnd",
        )

        # finished both in dbnd and airflow => nothing
        runtime_syncer.sync_once()
        expect_changes(
            runtime_syncer,
            init=0,
            update=0,
            case="finished both in dbnd and airflow => nothing",
        )
