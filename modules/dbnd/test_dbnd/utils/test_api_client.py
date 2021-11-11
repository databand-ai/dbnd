from contextlib import contextmanager
from unittest import TestCase
from unittest.mock import MagicMock, call, patch

import mock
import requests

from dbnd._core.utils.http.retry_policy import LinearRetryPolicy
from dbnd.utils.api_client import ApiClient


class TestApiClient(TestCase):
    base_url = "http://does-not-exist.local"
    creds = {"username": "dbnd_user", "password": "self.dbnd_password"}

    def setUp(self):
        self.network_request_mock = MagicMock()
        self.ok_response = MagicMock()
        self.ok_response.ok = True
        self.connection_error = requests.exceptions.ConnectionError("mocked error")

    def test_dont_fail_after_first_error(self):
        self.network_request_mock.side_effect = [
            self.connection_error,
            self.ok_response,
            self.ok_response,
        ]
        sut = self.create_sut(retries=2)
        try:
            with self.dont_sleep():
                sut.api_request("tracking/init_run", {})
        except Exception as e:
            self.fail(
                f"Api client should be resilient to failures, but instead got: {e}"
            )

    def create_sut(self, retries=None):
        sut = ApiClient(self.base_url, self.creds, default_max_retry=retries)
        sut._request = self.network_request_mock
        return sut

    def test_after_anon_session_created_should_login_on_next_request(self):
        self.network_request_mock.side_effect = self.ok_response
        sut = self.create_sut()
        try:
            with self.dont_sleep():
                sut.api_request("auth/ping", {}, requires_auth=False)
                sut.api_request("some/request", {}, requires_auth=True)
        except Exception as e:
            self.fail(
                f"Api client should be resilient to failures, but instead got: {e}"
            )
        assert self.network_request_mock.call_count == 3
        assert self.network_request_mock.mock_calls[1] == self.login_call()

    def test_api_client_should_retry_login_3_times(self):
        self.network_request_mock.side_effect = self.connection_error
        sut = self.create_sut(retries=3)
        try:
            with self.dont_sleep():
                sut.api_request("tracking/init_run", {})
        except:
            # Request will fail, this is expected
            pass

        _call = self.login_call()
        assert self.network_request_mock.call_count == 3
        self.network_request_mock.assert_has_calls([_call, _call, _call])

    def login_call(self):
        return call(
            "/api/v1/auth/login",
            method="POST",
            data=mock.ANY,
            headers=mock.ANY,
            query=mock.ANY,
            request_timeout=mock.ANY,
            session=mock.ANY,
        )

    @contextmanager
    def dont_sleep(self):
        with patch.object(LinearRetryPolicy, "seconds_to_sleep") as secs:
            # Do not sleep on CI
            secs.return_value = 0
            yield
