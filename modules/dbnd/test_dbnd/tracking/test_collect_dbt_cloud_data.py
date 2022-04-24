from unittest.mock import patch

from dbnd._core.tracking.dbt import collect_data_from_dbt_cloud


@patch("dbnd._core.tracking.dbt._report_dbt_metadata")
@patch("dbnd._core.tracking.dbt.DbtCloudApiClient")
class TestCollectDataFromDbtCloud:
    DBT_CLOUD_API_KEY = "my_dbt_cloud_api_key"
    DBT_CLOUD_ACCOUNT_ID = 5445
    DBT_CLOUD_RUN_ID = 1234

    def test_collect_dbt_data_without_run(
        self, dbt_cloud_api_client_mock, report_dbt_metadata_mock
    ):
        dbt_cloud_api_mocked_instance = dbt_cloud_api_client_mock.return_value
        dbt_cloud_api_mocked_instance.get_run.return_value = None
        collect_data_from_dbt_cloud(
            dbt_cloud_account_id=self.DBT_CLOUD_ACCOUNT_ID,
            dbt_cloud_api_token=self.DBT_CLOUD_API_KEY,
            dbt_job_run_id=self.DBT_CLOUD_RUN_ID,
        )

        dbt_cloud_api_mocked_instance.get_run.assert_called_with(self.DBT_CLOUD_RUN_ID)
        dbt_cloud_api_mocked_instance.get_run_results_artifact.assert_not_called()
        report_dbt_metadata_mock.assert_not_called()

    def test_collect_dbt_data_without_run_steps(
        self, dbt_cloud_api_client_mock, report_dbt_metadata_mock
    ):
        dbt_cloud_api_mocked_instance = dbt_cloud_api_client_mock.return_value
        dbt_cloud_api_mocked_instance.get_run.return_value = {"dummy_data": True}
        collect_data_from_dbt_cloud(
            dbt_cloud_account_id=self.DBT_CLOUD_ACCOUNT_ID,
            dbt_cloud_api_token=self.DBT_CLOUD_API_KEY,
            dbt_job_run_id=self.DBT_CLOUD_RUN_ID,
        )

        dbt_cloud_api_mocked_instance.get_run.assert_called_with(self.DBT_CLOUD_RUN_ID)
        dbt_cloud_api_mocked_instance.get_run_results_artifact.assert_not_called()
        report_dbt_metadata_mock.assert_called()

    def test_happy_path_with_run_steps(
        self, dbt_cloud_api_client_mock, report_dbt_metadata_mock
    ):
        dbt_cloud_api_mocked_instance = dbt_cloud_api_client_mock.return_value
        run_steps = [{"index": 1}, {"index": 2}, {"index": 3}]
        run_results_res = {"test": "res"}
        manifest_res = {"manifest": "www"}
        env = {"env": 123}

        dbt_cloud_api_mocked_instance.get_run.return_value = {"run_steps": run_steps}
        dbt_cloud_api_mocked_instance.get_run_results_artifact.return_value = (
            run_results_res
        )
        dbt_cloud_api_mocked_instance.get_manifest_artifact.return_value = manifest_res
        dbt_cloud_api_mocked_instance.get_environment.return_value = env
        expected_steps_with_results = [
            {**step, "run_results": run_results_res, "manifest": manifest_res}
            for step in run_steps
        ]
        expected_dbt_metadata_report = {
            "run_steps": expected_steps_with_results,
            "environment": env,
        }

        collect_data_from_dbt_cloud(
            dbt_cloud_account_id=self.DBT_CLOUD_ACCOUNT_ID,
            dbt_cloud_api_token=self.DBT_CLOUD_API_KEY,
            dbt_job_run_id=self.DBT_CLOUD_RUN_ID,
        )

        dbt_cloud_api_mocked_instance.get_run.assert_called_with(self.DBT_CLOUD_RUN_ID)
        assert dbt_cloud_api_mocked_instance.get_run_results_artifact.call_count == len(
            run_steps
        )
        report_dbt_metadata_mock.assert_called()
        report_dbt_metadata_mock.assert_called_with(expected_dbt_metadata_report)
