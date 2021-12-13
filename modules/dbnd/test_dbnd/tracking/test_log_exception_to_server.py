import pytest

from mock import patch

from dbnd import new_dbnd_context
from dbnd._core.log.external_exception_logging import capture_tracking_exception


class TestException(Exception):
    pass


@capture_tracking_exception
def raising_function():
    raise TestException("some error message")


class TestLogExceptionToServer:
    def test_log_exception_to_server(self):
        with new_dbnd_context(
            {"core": {"databand_access_token": "token", "databand_url": "some_url"}}
        ):
            with patch(
                "dbnd.utils.api_client.ApiClient._send_request"
            ) as send_request_mock:
                with pytest.raises(TestException):
                    raising_function()

                assert send_request_mock.call_count == 1
                call_kwargs = send_request_mock.call_args.kwargs
                assert call_kwargs["url"] == "/api/v1/log_exception"
                assert call_kwargs["json"]["source"] == "tracking-sdk"
                assert "in raising_function" in call_kwargs["json"]["stack_trace"]
                assert (
                    "TestException: some error message"
                    in call_kwargs["json"]["stack_trace"]
                )

    def test_log_exception_to_server_no_url(self):
        """ Shouldn't try to send data if api is not configured """
        with new_dbnd_context({"core": {"databand_url": ""}}):
            with patch(
                "dbnd.utils.api_client.ApiClient._send_request"
            ) as send_request_mock:
                with pytest.raises(TestException):
                    raising_function()

                assert not send_request_mock.called
