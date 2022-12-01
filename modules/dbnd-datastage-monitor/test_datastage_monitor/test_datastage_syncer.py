# © Copyright Databand.ai, an IBM Company 2022

from unittest.mock import MagicMock, patch

import pytest

from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.fetcher.multi_project_data_fetcher import (
    MultiProjectDataStageDataFetcher,
)
from dbnd_datastage_monitor.syncer.datastage_runs_syncer import DataStageRunsSyncer

from dbnd._core.utils.timezone import utcnow


@pytest.fixture
def runtime_syncer():
    syncer = DataStageRunsSyncer(
        config=DataStageServerConfig(
            source_name="test", source_type="datastage", tracking_source_uid="12345"
        ),
        tracking_service=None,
        data_fetcher=None,
    )
    with patch.object(
        syncer, "tracking_service", wraps=syncer.tracking_service
    ), patch.object(syncer, "data_fetcher", wraps=syncer.data_fetcher):
        yield syncer


class TestDatastageSyncer:
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
        self, runtime_syncer, new_runs, failed_runs, expected_total_runs
    ):
        runtime_syncer.error_handler.submit_run_request_retries(failed_runs)
        total_new_runs = runtime_syncer._append_failed_run_requests_for_retry(new_runs)
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
        self, runtime_syncer, runs_links, runs_to_fail
    ):
        project_runs = {"e9b6d8e2-5681-416f-9506-94b0849cfe8b": runs_links}
        fetcher_mock = MultiProjectDataStageDataFetcher(datastage_project_clients=[])
        fetcher_mock.get_full_runs = MagicMock(return_value=({}, runs_to_fail))
        runtime_syncer.data_fetcher = fetcher_mock
        runtime_syncer._init_runs_for_projects(project_runs, utcnow())
        expected_submitted_runs = runtime_syncer.error_handler.pull_run_request_retries(
            10
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
        runtime_syncer,
        run_links,
        failed_run_links,
        expected_min_start_time,
        expected_fail_runs_min_start_time,
    ):
        for run in failed_run_links:
            runtime_syncer.error_handler.submit_run_request_retry(run.get("run_link"))
        runtime_syncer.error_handler.pull_run_request_retries(batch_size=10)
        min_start_time = runtime_syncer.get_min_run_start_time(
            run_links + failed_run_links, utcnow()
        )
        assert str(min_start_time) == expected_min_start_time
