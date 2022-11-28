# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd_datastage_monitor.datastage_runs_error_handler.datastage_runs_error_handler import (
    DatastageRunsErrorQueue,
)


class TestDatastageRunsErrorQueue:
    def test_submit_and_pull_run(self):
        run_link = "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d476?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b"
        error_handler = DatastageRunsErrorQueue()
        error_handler.submit_failed_run(run_link)
        failed_runs = error_handler.pull_failed_runs(batch_size=10)
        failed_run = failed_runs[0]
        assert len(failed_runs) == 1
        assert failed_run.run_id == "cdc82817-4027-45b4-ad3b-ecd0bb50d476"
        failed_run.run_link = run_link
        failed_run.project_id = "e9b6d8e2-5681-416f-9506-94b0849cfe8b"

    @pytest.mark.parametrize(
        "number_of_runs_to_submit, expected_cached_retries_value, expected_number_of_runs_pulled",
        [[0, None, 0], [3, 3, 3], [4, None, 3]],
    )
    def test_submit_run_retry_update(
        self,
        number_of_runs_to_submit,
        expected_cached_retries_value,
        expected_number_of_runs_pulled,
    ):
        run_link = "https://cpd-ds.apps.datastageaws4.4qj6.p1.openshiftapps.com/v2/assets/cdc82817-4027-45b4-ad3b-ecd0bb50d476?project_id=e9b6d8e2-5681-416f-9506-94b0849cfe8b"
        error_handler = DatastageRunsErrorQueue()
        for i in range(number_of_runs_to_submit):
            error_handler.submit_failed_run(run_link)
        failed_runs = error_handler.pull_failed_runs(batch_size=10)
        retries = error_handler.failed_runs_retries_cache.get(run_link)
        assert retries == expected_cached_retries_value
        assert len(failed_runs) == expected_number_of_runs_pulled
        for i, failed_run in enumerate(failed_runs):
            assert failed_run.run_id == "cdc82817-4027-45b4-ad3b-ecd0bb50d476"
            assert failed_run.run_link == run_link
            assert failed_run.project_id == "e9b6d8e2-5681-416f-9506-94b0849cfe8b"
            assert failed_run.retry_attempt == i + 1
