# © Copyright Databand.ai, an IBM Company 2022
import json

from typing import Type
from unittest.mock import MagicMock, patch

import attr
import pytest

from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.datastage_client.datastage_api_client import (
    DataStageApiHttpClient,
)
from dbnd_datastage_monitor.datastage_client.datastage_assets_client import (
    DataStageAssetsClient,
)
from dbnd_datastage_monitor.fetcher.multi_project_data_fetcher import (
    MultiProjectDataStageDataFetcher,
)
from dbnd_datastage_monitor.syncer.datastage_runs_syncer import DataStageRunsSyncer
from dbnd_datastage_monitor.tracking_service.dbnd_datastage_tracking_service import (
    DbndDataStageTrackingService,
)

from dbnd import relative_path
from dbnd._core.utils.timezone import utcnow


RUNNING_STATE = "running"
FINISHED_STATE = "success"
PROJECT_ID = "8353f004-dc2e-43d5-9199-a923f15dc633"


@attr.s(auto_attribs=True)
class MockDatastageRun:
    id: int
    state: str
    run_link: str


def load_full_runs_data(path):
    return json.load(open(relative_path(__file__, path)))


class MockDatastageFetcher(MultiProjectDataStageDataFetcher):
    def __init__(self, client_dicts):
        asset_clients = [
            DataStageAssetsClient(client=DataStageApiHttpClient(**client_dict))
            for client_dict in client_dicts
        ]
        super(MockDatastageFetcher, self).__init__(asset_clients)
        self.datastage_runs = []

    def get_run_ids_to_sync_from_datastage(self, dbnd_last_run_id):
        return [
            datastage_run.id
            for datastage_run in self.datastage_runs
            if datastage_run.id > dbnd_last_run_id
        ]

    def get_last_seen_run_id(self):
        all_ids = [datastage_run.id for datastage_run in self.datastage_runs]
        return max(all_ids) if all_ids else None

    def get_full_datastage_runs(self, datastage_run_ids):
        return [
            Datastage_run
            for Datastage_run in self.datastage_runs
            if Datastage_run.id in datastage_run_ids
        ]

    def get_active_datastage_runs(self):
        return [
            run.run_link
            for run in self.datastage_runs
            if extract_project_id_from_url(run.run_link)
        ]

    def get_full_runs(self, runs, project_id: str):
        datastage_runs_full_data = load_full_runs_data(
            path="mocks/syncer/datastage_runs_full_data.json"
        )
        failed_runs = []
        return datastage_runs_full_data, failed_runs


class MockDatastageTrackingService(DbndDataStageTrackingService):
    def __init__(
        self,
        monitor_type: str,
        tracking_source_uid: str,
        server_monitor_config: Type[DataStageServerConfig],
    ):
        DbndDataStageTrackingService.__init__(
            self,
            monitor_type,
            tracking_source_uid,
            server_monitor_config=server_monitor_config,
        )
        self.datastage_runs = []
        self.last_seen_run_id = None
        self.last_seen_date = None

    def init_datastage_runs(self, datastage_runs_full_data):
        pass

    def update_datastage_runs(self, datastage_runs_full_data):
        pass

    def update_last_sync_time(self):
        pass

    def update_last_seen_values(self, date):
        self.last_seen_date = date

    def get_last_seen_date(self):
        return self.last_seen_date

    def get_running_datastage_runs(self):
        return [
            datastage_run.run_link
            for datastage_run in self.datastage_runs
            if datastage_run.state == RUNNING_STATE
        ]


def extract_project_id_from_url(url: str):
    return PROJECT_ID


@pytest.fixture
def mock_datastage_tracking_service() -> MockDatastageTrackingService:
    yield MockDatastageTrackingService(
        "datastage", "12345", server_monitor_config=DataStageServerConfig
    )


@pytest.fixture
def mock_datastage_fetcher() -> MockDatastageFetcher:
    yield MockDatastageFetcher(
        client_dicts=[
            {
                "host_name": "https://test.run.datastage.ibm",
                "authentication_provider_url": None,
                "authentication_type": "cloud-iam-auth",
                "api_key": "RETRACTED",  # pragma: allowlist secret
                "project_id": PROJECT_ID,
                "page_size": 200,
            }
        ]
    )


@pytest.fixture
def datastage_runtime_syncer(mock_datastage_fetcher, mock_datastage_tracking_service):
    syncer = DataStageRunsSyncer(
        config=DataStageServerConfig(
            source_name="test_syncer",
            source_type="datastage",
            tracking_source_uid="12345",
            sync_interval=10,
            fetching_interval_in_minutes=10,
        ),
        tracking_service=mock_datastage_tracking_service,
        data_fetcher=mock_datastage_fetcher,
    )
    with patch.object(syncer, "refresh_config", new=lambda *args: None), patch.object(
        syncer, "tracking_service", wraps=syncer.tracking_service
    ), patch.object(syncer, "data_fetcher", wraps=syncer.data_fetcher):
        yield syncer


def expect_changes(
    runtime_syncer,
    init=0,
    update=0,
    is_dbnd_empty=False,
    is_datastage_empty=False,
    reset=False,
):
    patched_data_fetcher = runtime_syncer.data_fetcher
    patched_tracking_service = runtime_syncer.tracking_service

    if is_dbnd_empty:
        if is_datastage_empty:
            assert patched_tracking_service.update_last_seen_values.call_count == 0
        else:
            assert patched_tracking_service.update_last_seen_values.call_count == 1
    else:
        if init == 0:
            assert patched_tracking_service.update_last_seen_values.call_count == 0
        else:
            assert patched_tracking_service.update_last_seen_values.call_count == 1

    assert patched_data_fetcher.get_full_runs.call_count == init + update
    assert patched_tracking_service.init_datastage_runs.call_count == init
    assert patched_tracking_service.update_datastage_runs.call_count == update

    if reset:
        patched_data_fetcher.reset_mock()
        patched_tracking_service.reset_mock()


class TestDatastageRunsSyncer:
    def test_sync_datastage_runs(
        self, datastage_runtime_syncer, mock_datastage_tracking_service
    ):
        # first sync -> just to set the last_seen_str
        expect_changes(
            datastage_runtime_syncer,
            init=0,
            update=0,
            is_dbnd_empty=True,
            is_datastage_empty=True,
        )
        datastage_runtime_syncer.sync_once()
        expect_changes(
            datastage_runtime_syncer,
            init=0,
            update=0,
            is_dbnd_empty=True,
            is_datastage_empty=False,
            reset=True,
        )

        # second sync with first finished run
        old_datastage_run = MockDatastageRun(
            id=1,
            state=FINISHED_STATE,
            run_link="https://test.run.datastage.ibm/v2/assets/finished_run",
        )
        mock_datastage_tracking_service.datastage_runs.append(old_datastage_run)
        datastage_runtime_syncer.data_fetcher.get_runs_to_sync.return_value = {
            PROJECT_ID: {
                "some_run_id": f"https://test.run.datastage.ibm/v2/assets/finished_run?project_id={PROJECT_ID}"
            }
        }
        datastage_runtime_syncer.sync_once()
        expect_changes(datastage_runtime_syncer, init=1, update=0, is_dbnd_empty=False)
        datastage_runtime_syncer.data_fetcher.get_full_runs.assert_called_with(
            [
                f"https://test.run.datastage.ibm/v2/assets/finished_run?project_id={PROJECT_ID}"
            ],
            PROJECT_ID,
        )

    @pytest.mark.parametrize(
        "new_runs, failed_runs, expected_total_runs",
        [
            [
                {},
                [
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d476?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d477?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d478?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                ],
                {
                    "e9b6d8e2-5681-416f-9506-94b0849cfe8b": {
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d476": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d476?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d477": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d477?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d478": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d478?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    }
                },
            ],
            [
                {
                    "e9b6d8e2-5681-416f-9506-94b0849cfe8b": {
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d60": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b"
                    },
                    "e9b6d8e2-5681-416f-9506-94b0849cfe8c": {
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d60": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c"
                    },
                },
                [
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d476?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d477?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d478?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                ],
                {
                    "e9b6d8e2-5681-416f-9506-94b0849cfe8b": {
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d60": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d476": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d476?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d477": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d477?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d478": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d478?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    },
                    "e9b6d8e2-5681-416f-9506-94b0849cfe8c": {
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d60": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c"
                    },
                },
            ],
            [{}, [], {}],
            [
                {
                    "e9b6d8e2-5681-416f-9506-94b0849cfe8b": {
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d60": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b"
                    },
                    "e9b6d8e2-5681-416f-9506-94b0849cfe8c": {
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d60": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c"
                    },
                },
                [],
                {
                    "e9b6d8e2-5681-416f-9506-94b0849cfe8b": {
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d60": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b"
                    },
                    "e9b6d8e2-5681-416f-9506-94b0849cfe8c": {
                        "cdc82817-4027-45b4-ad3b-ecd0bb50d60": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c"
                    },
                },
            ],
        ],
    )
    def test_append_failed_run_requests_for_retry(
        self, datastage_runtime_syncer, new_runs, failed_runs, expected_total_runs
    ):
        datastage_runtime_syncer.error_handler.submit_run_request_retries(failed_runs)
        total_new_runs = datastage_runtime_syncer._append_failed_run_requests_for_retry(
            new_runs
        )
        assert total_new_runs == expected_total_runs

    @pytest.mark.parametrize(
        "runs_links, runs_to_fail",
        [
            [
                [
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c",
                ],
                [
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c",
                ],
            ],
            [[], []],
            [
                [
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c",
                ],
                [],
            ],
        ],
    )
    def test_get_and_submit_failed_run_request_retries(
        self, datastage_runtime_syncer, runs_links, runs_to_fail
    ):
        project_runs = {"e9b6d8e2-5681-416f-9506-94b0849cfe8b": runs_links}
        fetcher_mock = MultiProjectDataStageDataFetcher(datastage_project_clients=[])
        fetcher_mock.get_full_runs = MagicMock(return_value=({}, runs_to_fail))
        datastage_runtime_syncer.data_fetcher = fetcher_mock
        datastage_runtime_syncer._init_runs_for_projects(project_runs, utcnow())
        expected_submitted_runs = (
            datastage_runtime_syncer.error_handler.pull_run_request_retries(10)
        )
        for i, run in enumerate(runs_to_fail):
            assert expected_submitted_runs[i].run_link == run

    @pytest.mark.parametrize(
        "run_links, failed_run_links, expected_min_start_time, expected_fail_runs_min_start_time",
        [
            [
                [
                    {
                        "run_link": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/df653d16-a04c-408e-8dd6-aab2b2822a9a?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                        "run_info": {
                            "metadata": {"created_at": "2022-11-06T15:07:36Z"}
                        },
                    },
                    {
                        "run_link": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/df653d16-a04c-408e-8dd6-aab2b2822a9a?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c",
                        "run_info": {
                            "metadata": {"created_at": "2022-11-06T15:08:36Z"}
                        },
                    },
                ],
                [
                    {
                        "run_link": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/df653d16-a04c-408e-8dd6-aab2b2822a9a?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8d",
                        "run_info": {
                            "metadata": {"created_at": "2022-11-06T15:09:36Z"}
                        },
                    },
                    {
                        "run_link": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/df653d16-a04c-408e-8dd6-aab2b2822a9a?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8e",
                        "run_info": {
                            "metadata": {"created_at": "2022-11-06T15:10:36Z"}
                        },
                    },
                ],
                "2022-11-06T15:07:36+00:00",
                "2022-11-06T15:09:36+00:00",
            ],
            [
                [
                    {
                        "run_link": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/df653d16-a04c-408e-8dd6-aab2b2822a9a?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                        "run_info": {
                            "metadata": {"created_at": "2022-11-06T15:07:36Z"}
                        },
                    },
                    {
                        "run_link": "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/df653d16-a04c-408e-8dd6-aab2b2822a9a?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c",
                        "run_info": {
                            "metadata": {"created_at": "2022-11-06T15:08:36Z"}
                        },
                    },
                ],
                [],
                "2022-11-06T15:07:36+00:00",
                "None",
            ],
            [[], [], "None", "None"],
        ],
    )
    def test_get_min_start_time(
        self,
        datastage_runtime_syncer,
        run_links,
        failed_run_links,
        expected_min_start_time,
        expected_fail_runs_min_start_time,
    ):
        for run in failed_run_links:
            datastage_runtime_syncer.error_handler.submit_run_request_retry(
                run.get("run_link")
            )
        datastage_runtime_syncer.error_handler.pull_run_request_retries(batch_size=10)
        min_start_time = datastage_runtime_syncer.get_min_run_start_time(
            run_links + failed_run_links, utcnow()
        )
        assert str(min_start_time) == expected_min_start_time

    @pytest.mark.parametrize(
        "runs, update_runs_expected_call_count, update_last_sync_time_expected_call_count",
        [
            [
                [
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8d",
                ],
                3,
                3,
            ],
            [
                [
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8c",
                ],
                2,
                2,
            ],
            [
                [
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                    "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d460?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b",
                ],
                1,
                1,
            ],
            [[], 0, 0],
            [None, 0, 0],
        ],
    )
    def test_update_runs(
        self,
        datastage_runtime_syncer,
        runs,
        update_runs_expected_call_count,
        update_last_sync_time_expected_call_count,
    ):
        datastage_runtime_syncer._update_runs(runs)
        patched_tracking_service = datastage_runtime_syncer.tracking_service
        assert (
            patched_tracking_service.update_datastage_runs.call_count
            == update_runs_expected_call_count
        )
        assert (
            patched_tracking_service.update_last_sync_time.call_count
            == update_last_sync_time_expected_call_count
        )
        if update_runs_expected_call_count:
            patched_tracking_service.update_datastage_runs.assert_called_with(
                load_full_runs_data(path="mocks/syncer/datastage_runs_full_data.json")
            )
