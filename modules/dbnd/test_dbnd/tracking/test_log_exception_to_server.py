# © Copyright Databand.ai, an IBM Company 2022

import gzip
import json

import pytest

from mock import patch

from dbnd import new_dbnd_context
from dbnd._core.log.external_exception_logging import capture_tracking_exception


class RandomException(Exception):
    pass


@capture_tracking_exception
def raising_function():
    raise RandomException("some error message")


class TestLogExceptionToServer:
    def test_log_exception_to_server(self):
        with new_dbnd_context(
            {"core": {"databand_access_token": "token", "databand_url": "some_url"}}
        ):
            with patch(
                "dbnd.utils.api_client.ApiClient._send_request"
            ) as send_request_mock:
                with pytest.raises(RandomException):
                    raising_function()

                assert send_request_mock.call_count == 1
                call_kwargs = send_request_mock.call_args.kwargs
                assert call_kwargs["url"] == "/api/v1/log_exception"
                data_dict = json.loads(gzip.decompress(call_kwargs["data"]))
                assert data_dict["source"] == "tracking-sdk"
                assert "in raising_function" in data_dict["stack_trace"]
                assert "RandomException: some error message" in data_dict["stack_trace"]

    def test_log_exception_to_server_no_url(self):
        """Shouldn't try to send data if api is not configured"""
        with new_dbnd_context({"core": {"databand_url": ""}}):
            with patch(
                "dbnd.utils.api_client.ApiClient._send_request"
            ) as send_request_mock:
                with pytest.raises(RandomException):
                    raising_function()

                assert not send_request_mock.called
