from unittest import TestCase
from unittest.mock import MagicMock, call

from dbt_monitor.tracking_service.dbt_cloud_tracking_service import (
    DbtCloudTrackingService,
)


class TestDbtCloudTrackingService(TestCase):
    def setUp(self) -> None:
        self.mock_dbt_cloud_api_client = MagicMock()
        self.dbt_runs_syncer = DbtCloudTrackingService(self.mock_dbt_cloud_api_client)

    def mock_return_runs(self, num_of_runs):
        ans = []
        for i in range(num_of_runs):
            ans.append({"id": i})

        return ans

    def test_response_with_synced_run(self):
        last_synced_run_id = 9
        runs_from_dbt = self.mock_return_runs(10)
        self.mock_dbt_cloud_api_client.list_runs.return_value = runs_from_dbt
        expected_returned_runs = [
            run for run in runs_from_dbt if run["id"] != last_synced_run_id
        ]

        dbt_runs = self.dbt_runs_syncer.get_new_dbt_runs_with_meta_data(
            last_synced_run_id
        )

        # Assertion
        self.mock_dbt_cloud_api_client.list_runs.assert_called_once_with(
            limit=10, offset=0
        )
        self.mock_dbt_cloud_api_client.get_run.call_count == len(expected_returned_runs)
        assert not self.mock_dbt_cloud_api_client.get_run_results_artifact.called
        assert dbt_runs == expected_returned_runs

    def test_synced_run_on_second_fetch(self):
        last_synced_run_id = 14
        runs_from_dbt = self.mock_return_runs(20)
        half = len(runs_from_dbt) // 2
        first_runs_response, second_runs_response = (
            runs_from_dbt[:half],
            runs_from_dbt[half:],
        )
        self.mock_dbt_cloud_api_client.list_runs.side_effect = [
            first_runs_response,
            second_runs_response,
        ]
        expected_returned_runs = [
            run for run in runs_from_dbt if run["id"] < last_synced_run_id
        ]
        list_runs_calls = [call(limit=10, offset=0), call(limit=10, offset=10)]

        dbt_runs = self.dbt_runs_syncer.get_new_dbt_runs_with_meta_data(
            last_synced_run_id
        )

        # Assertion
        self.mock_dbt_cloud_api_client.list_runs.assert_has_calls(list_runs_calls)
        self.mock_dbt_cloud_api_client.list_runs.call_count == 2
        self.mock_dbt_cloud_api_client.get_run.call_count == len(expected_returned_runs)
        assert not self.mock_dbt_cloud_api_client.get_run_results_artifact.called
        assert dbt_runs == expected_returned_runs

    def test_one_iteration_on_first_call(self):
        runs_from_dbt = self.mock_return_runs(10)
        self.mock_dbt_cloud_api_client.list_runs.return_value = runs_from_dbt

        dbt_runs = self.dbt_runs_syncer.get_new_dbt_runs_with_meta_data()

        # Assertion
        self.mock_dbt_cloud_api_client.list_runs.assert_called_once_with(
            limit=10, offset=0
        )
        self.mock_dbt_cloud_api_client.get_run.call_count == len(dbt_runs)
        assert not self.mock_dbt_cloud_api_client.get_run_results_artifact.called
        assert dbt_runs == runs_from_dbt

    def test_empty_response_from_dbt_cloud(self):
        runs_from_dbt = []
        self.mock_dbt_cloud_api_client.list_runs.return_value = runs_from_dbt
        expected_returned_runs = []

        dbt_runs = self.dbt_runs_syncer.get_new_dbt_runs_with_meta_data()

        # Assertion
        self.mock_dbt_cloud_api_client.list_runs.assert_called_once_with(
            limit=10, offset=0
        )
        self.mock_dbt_cloud_api_client.get_run.call_count == len(expected_returned_runs)
        assert not self.mock_dbt_cloud_api_client.get_run_results_artifact.called
        assert dbt_runs == expected_returned_runs
