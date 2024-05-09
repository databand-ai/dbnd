# © Copyright Databand.ai, an IBM Company 2022

from unittest.mock import patch

from mock.mock import ANY, MagicMock

from dbnd.providers.dbt.dbt_cloud import collect_data_from_dbt_cloud


@patch("dbnd.providers.dbt.dbt_cloud._get_tracker")
@patch("dbnd.providers.dbt.dbt_cloud.DbtCloudApiClient")
class TestCollectDataFromDbtCloud:
    DBT_CLOUD_API_KEY = "my_dbt_cloud_api_key"  # pragma: allowlist secret
    DBT_DEFAULT_API_URL = "https://cloud.getdbt.com"
    DBT_CLOUD_ACCOUNT_ID = 5445
    DBT_CLOUD_RUN_ID = 1234

    def test_collect_data_from_dbt_cloud_no_tracker(
        self, dbt_cloud_api_client_mock, get_tracker_mock
    ):
        get_tracker_mock.return_value = None

        collect_data_from_dbt_cloud(
            dbt_cloud_account_id="dummy_account_id",
            dbt_cloud_api_token="dummy_api_token",
            dbt_job_run_id="dummy_run_id",
            dbt_api_url="dummy_url",
        )

        dbt_cloud_api_client_mock.assert_not_called()

    def test_collect_dbt_data_without_run(
        self, dbt_cloud_api_client_mock, get_tracker_mock
    ):
        get_tracker_mock.return_value = MagicMock()
        dbt_cloud_api_mocked_instance = dbt_cloud_api_client_mock.return_value
        dbt_cloud_api_mocked_instance.get_run.return_value = None
        collect_data_from_dbt_cloud(
            dbt_cloud_account_id=self.DBT_CLOUD_ACCOUNT_ID,
            dbt_cloud_api_token=self.DBT_CLOUD_API_KEY,
            dbt_job_run_id=self.DBT_CLOUD_RUN_ID,
            dbt_api_url=self.DBT_DEFAULT_API_URL,
        )

        dbt_cloud_api_mocked_instance.get_run.assert_called_with(
            run_id=self.DBT_CLOUD_RUN_ID
        )
        dbt_cloud_api_mocked_instance.get_run_results_artifact.assert_not_called()
        get_tracker_mock.return_value.log_dbt_metadata.assert_not_called()

    def test_collect_dbt_data_without_run_id(
        self, dbt_cloud_api_client_mock, get_tracker_mock
    ):
        dbt_cloud_api_mocked_instance = dbt_cloud_api_client_mock.return_value
        dbt_cloud_api_mocked_instance.get_run.return_value = None
        collect_data_from_dbt_cloud(
            dbt_cloud_account_id=self.DBT_CLOUD_ACCOUNT_ID,
            dbt_cloud_api_token=self.DBT_CLOUD_API_KEY,
            dbt_job_run_id="",
            dbt_api_url=self.DBT_DEFAULT_API_URL,
        )

        dbt_cloud_api_mocked_instance.get_run.assert_not_called()
        dbt_cloud_api_mocked_instance.get_run_results_artifact.assert_not_called()
        dbt_cloud_api_mocked_instance.get_environment.assert_not_called()
        get_tracker_mock.return_value.log_dbt_metadata.assert_not_called()

    def test_collect_dbt_data_without_run_steps(
        self, dbt_cloud_api_client_mock, get_tracker_mock
    ):
        dbt_cloud_api_mocked_instance = dbt_cloud_api_client_mock.return_value
        dbt_cloud_api_mocked_instance.get_run.return_value = {"dummy_data": True}
        collect_data_from_dbt_cloud(
            dbt_cloud_account_id=self.DBT_CLOUD_ACCOUNT_ID,
            dbt_cloud_api_token=self.DBT_CLOUD_API_KEY,
            dbt_job_run_id=self.DBT_CLOUD_RUN_ID,
            dbt_api_url=self.DBT_DEFAULT_API_URL,
        )

        dbt_cloud_api_mocked_instance.get_run.assert_called_with(
            run_id=self.DBT_CLOUD_RUN_ID
        )
        dbt_cloud_api_mocked_instance.get_run_results_artifact.assert_not_called()
        get_tracker_mock.return_value.log_dbt_metadata.assert_called_with(
            dbt_metadata={
                "dummy_data": True,
                "environment": ANY,
                "reported_from": "dbt_cloud",
            }
        )

    def test_happy_path_with_run_steps(
        self, dbt_cloud_api_client_mock, get_tracker_mock
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
            "reported_from": "dbt_cloud",
        }

        collect_data_from_dbt_cloud(
            dbt_cloud_account_id=self.DBT_CLOUD_ACCOUNT_ID,
            dbt_cloud_api_token=self.DBT_CLOUD_API_KEY,
            dbt_job_run_id=self.DBT_CLOUD_RUN_ID,
            dbt_api_url=self.DBT_DEFAULT_API_URL,
        )

        dbt_cloud_api_mocked_instance.get_run.assert_called_with(
            run_id=self.DBT_CLOUD_RUN_ID
        )
        assert dbt_cloud_api_mocked_instance.get_run_results_artifact.call_count == len(
            run_steps
        )
        get_tracker_mock.return_value.log_dbt_metadata.assert_called()
        get_tracker_mock.return_value.log_dbt_metadata.assert_called_with(
            dbt_metadata=expected_dbt_metadata_report
        )

    def test_run_step_with_no_artifacts(
        self, dbt_cloud_api_client_mock, get_tracker_mock
    ):
        dbt_cloud_api_mocked_instance = dbt_cloud_api_client_mock.return_value
        run_steps = [{"index": 1}, {"index": 2}, {"index": 3}]
        env = {"env": 123}

        dbt_cloud_api_mocked_instance.get_run.return_value = {"run_steps": run_steps}
        dbt_cloud_api_mocked_instance.get_run_results_artifact.return_value = None
        dbt_cloud_api_mocked_instance.get_manifest_artifact.return_value = None
        dbt_cloud_api_mocked_instance.get_environment.return_value = env
        expected_steps_with_results = [{**step} for step in run_steps]
        expected_dbt_metadata_report = {
            "run_steps": expected_steps_with_results,
            "environment": env,
            "reported_from": "dbt_cloud",
        }

        collect_data_from_dbt_cloud(
            dbt_cloud_account_id=self.DBT_CLOUD_ACCOUNT_ID,
            dbt_cloud_api_token=self.DBT_CLOUD_API_KEY,
            dbt_job_run_id=self.DBT_CLOUD_RUN_ID,
            dbt_api_url=self.DBT_DEFAULT_API_URL,
        )

        dbt_cloud_api_mocked_instance.get_run.assert_called_with(
            run_id=self.DBT_CLOUD_RUN_ID
        )
        assert dbt_cloud_api_mocked_instance.get_run_results_artifact.call_count == len(
            run_steps
        )
        get_tracker_mock.return_value.log_dbt_metadata.assert_called()
        get_tracker_mock.return_value.log_dbt_metadata.assert_called_with(
            dbt_metadata=expected_dbt_metadata_report
        )

    def test_run_not_found(self, dbt_cloud_api_client_mock, get_tracker_mock):
        dbt_cloud_api_mocked_instance = dbt_cloud_api_client_mock.return_value
        dbt_cloud_api_mocked_instance.get_run.return_value = None

        collect_data_from_dbt_cloud(
            dbt_cloud_account_id=self.DBT_CLOUD_ACCOUNT_ID,
            dbt_cloud_api_token=self.DBT_CLOUD_API_KEY,
            dbt_job_run_id=self.DBT_CLOUD_RUN_ID,
            dbt_api_url=self.DBT_DEFAULT_API_URL,
        )

        dbt_cloud_api_mocked_instance.get_run.assert_called_with(
            run_id=self.DBT_CLOUD_RUN_ID
        )
        dbt_cloud_api_mocked_instance.get_environment.assert_not_called()
        dbt_cloud_api_mocked_instance.get_run_results_artifact.assert_not_called()
        get_tracker_mock.return_value.log_dbt_metadata.assert_not_called()
