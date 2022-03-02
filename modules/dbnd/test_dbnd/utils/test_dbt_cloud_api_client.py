from http import HTTPStatus
from unittest.mock import MagicMock

import pytest

from requests.models import Response

from dbnd.utils.dbt_cloud_api_client import DbtCloudApiClient


class TestDbtCloudApiClient:
    DBT_CLOUD_API_KEY = "my_dbt_cloud_api_key"
    DBT_CLOUD_ACCOUNT_ID = 5445

    def setUp(self):
        self.send_request_mock = MagicMock()

    def test_get_run(self):
        self.setUp()
        run_id = 1234
        dbt_cloud_client = DbtCloudApiClient(
            self.DBT_CLOUD_ACCOUNT_ID, self.DBT_CLOUD_API_KEY
        )
        dbt_cloud_client.send_request = self.send_request_mock
        self.send_request_mock.return_value = {"run_id": run_id}
        expected_endpoint = f"{dbt_cloud_client.administrative_api_url}{self.DBT_CLOUD_ACCOUNT_ID}/runs/{run_id}"
        expected_params = {"include_related": '["run_steps"]'}
        dbt_cloud_client.get_run(run_id)
        self.send_request_mock.assert_called()
        self.send_request_mock.assert_called_with(
            endpoint=expected_endpoint, data=expected_params
        )

    def test_get_run_results_artifact(self):
        self.setUp()
        run_id = 1234
        dbt_cloud_client = DbtCloudApiClient(
            self.DBT_CLOUD_ACCOUNT_ID, self.DBT_CLOUD_API_KEY
        )
        dbt_cloud_client.send_request = self.send_request_mock
        self.send_request_mock.return_value = {"run_id": run_id}
        expected_endpoint = f"{dbt_cloud_client.administrative_api_url}{self.DBT_CLOUD_ACCOUNT_ID}/runs/{run_id}/artifacts/run_results.json"
        expected_params = {"step": 1}
        dbt_cloud_client.get_run_results_artifact(run_id)
        self.send_request_mock.assert_called()
        self.send_request_mock.assert_called_with(
            endpoint=expected_endpoint, data=expected_params
        )

    def test_query_dbt_run_results(self):
        self.setUp()
        run_id = 1234
        job_id = 5566
        dbt_cloud_client = DbtCloudApiClient(
            self.DBT_CLOUD_ACCOUNT_ID, self.DBT_CLOUD_API_KEY
        )
        dbt_cloud_client.send_request = self.send_request_mock
        expected_query = (
            "{\nmodels(runId: %s,jobId: %s){\nuniqueId,\nexecutionTime,\nstatus\n}\n}"
            % (run_id, job_id)
        )
        dbt_cloud_client.query_dbt_run_results(job_id, run_id)
        self.send_request_mock.assert_called()
        self.send_request_mock.assert_called_with(
            endpoint=dbt_cloud_client.metadata_api_url,
            method="POST",
            data={"query": expected_query},
        )

    def test_query_dbt_run_results(self):
        self.setUp()
        run_id = 1234
        job_id = 5566
        dbt_cloud_client = DbtCloudApiClient(
            self.DBT_CLOUD_ACCOUNT_ID, self.DBT_CLOUD_API_KEY
        )
        dbt_cloud_client.send_request = self.send_request_mock
        expected_query = "{\ntests(runId: %s,jobId: %s){\nuniqueId,\nstatus\n}\n}" % (
            run_id,
            job_id,
        )
        dbt_cloud_client.query_dbt_test_results(job_id, run_id)
        self.send_request_mock.assert_called()
        self.send_request_mock.assert_called_with(
            endpoint=dbt_cloud_client.metadata_api_url,
            method="POST",
            data={"query": expected_query},
        )

    def test_get_run_with_connection_error_exception(self):
        self.setUp()
        run_id = 1234
        dbt_cloud_client = DbtCloudApiClient(
            self.DBT_CLOUD_ACCOUNT_ID, self.DBT_CLOUD_API_KEY
        )
        session_mock = MagicMock()
        session_mock.side_effect = ConnectionError
        dbt_cloud_client.session.get = session_mock
        expected_endpoint = f"{dbt_cloud_client.administrative_api_url}{self.DBT_CLOUD_ACCOUNT_ID}/runs/{run_id}"
        expected_params = {"include_related": '["run_steps"]'}

        res = dbt_cloud_client.get_run(run_id)
        session_mock.assert_called_with(url=expected_endpoint, params=expected_params)
        assert res is None

    @pytest.mark.parametrize(
        "status_code,content",
        [
            (HTTPStatus.INTERNAL_SERVER_ERROR.value, "500 Internal server error"),
            (HTTPStatus.BAD_GATEWAY.value, "502 Bad Gateway"),
            (HTTPStatus.SERVICE_UNAVAILABLE.value, "503 Service Unavailable"),
            (HTTPStatus.FORBIDDEN.value, "Forbidden"),
            (HTTPStatus.BAD_REQUEST.value, "400 Bad Request"),
            (HTTPStatus.NOT_FOUND.value, "404 Not Found"),
        ],
    )
    def test_get_run_with_dbt_cloud_bad_status_code(self, status_code, content):
        self.setUp()
        run_id = 1234
        dbt_cloud_client = DbtCloudApiClient(
            self.DBT_CLOUD_ACCOUNT_ID, self.DBT_CLOUD_API_KEY
        )
        server_error_response = Response()
        server_error_response.status_code = status_code
        server_error_response._content = content
        session_mock = MagicMock()
        session_mock.get.return_value = server_error_response
        dbt_cloud_client.session = session_mock
        res = dbt_cloud_client.get_run(run_id)
        assert res is None
