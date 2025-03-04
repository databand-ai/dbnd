# © Copyright Databand.ai, an IBM Company 2022
from unittest.mock import patch

import pytest

from dbnd_monitor.error_handling.errors import ClientConnectionError
from test_dbnd_monitor.plugins.mock_generic_syncer import WithGenericSyncer
from test_dbnd_monitor.suites import (
    WhenComponentErrorDoesNotSupported,
    WhenComponentErrorSupportedByBothSides,
    WhenComponentErrorSupportedByMonitorOnly,
    WhenComponentErrorSupportedByWebServerOnly,
)


@pytest.fixture
def raise_exception(request):
    target = request.instance.generic_syncer
    with patch.object(target, "_sync_once", side_effect=Exception("Error")):
        yield


@pytest.fixture
def raise_exception_ignorable(request):
    target = request.instance.generic_syncer
    with patch.object(target, "_sync_once", side_effect=ClientConnectionError("Error")):
        yield


class Suite(WithGenericSyncer):
    @property
    def expected_api_endpoint(self):
        return (
            f"integrations/{self.generic_syncer.config.uid}/error?"
            f"type={self.generic_syncer.config.source_type}"
        )

    def act(self):
        """The method triggers the `sync_once` call for the Suite"""
        self.generic_syncer.sync_once()

    def assert_call_to_log_exception_endpoint(self, call):
        assert call.kwargs["endpoint"] == "log_exception"
        assert call.kwargs["method"] == "POST"

    def assert_call_to_integrations_endpoint(self, call):
        assert call.kwargs["endpoint"] == self.expected_api_endpoint
        assert call.kwargs["method"] == "PATCH"

    @pytest.mark.usefixtures("raise_exception")
    def test_capture_component_exception(self):
        """
        Exception reported to both `log_exception` and `integrations` endpoints
        """
        self.act()

        assert self.api_client.api_request.call_count == 2
        call_0, call_1 = self.api_client.api_request.call_args_list

        self.assert_call_to_log_exception_endpoint(call_0)
        self.assert_call_to_integrations_endpoint(call_1)

    @pytest.mark.usefixtures("raise_exception_ignorable")
    def test_ignore_capture_component_exception(self):
        """
        Exceptions from `ERRORS_TO_IGNORE` list are not reported
        to the `log_exception` endpoint"""
        self.act()

        assert self.api_client.api_request.call_count == 1
        (call_0,) = self.api_client.api_request.call_args_list

        self.assert_call_to_integrations_endpoint(call_0)


class TestCaptureExceptionDefault(Suite, WhenComponentErrorDoesNotSupported):
    pass


class TestCaptureExceptionWithWebServerSupport(
    Suite, WhenComponentErrorSupportedByWebServerOnly
):
    pass


class TestCaptureExceptionWithMonitorSupport(
    Suite, WhenComponentErrorSupportedByMonitorOnly
):
    pass


class TestCaptureComponentException(Suite, WhenComponentErrorSupportedByBothSides):
    """
    New flow assumes `ReportError.report_errors` method is being called
    explicitly in order to send collected errors to the web server, see
    `MultiServerMonitor.sync_component` method which performs sequentially
    `sync_once` and then `report_errors`.
    """

    def act(self):
        """
        This scenario needs the sequesnce of `sync_once` and `report_errors`
        to be called in order to prepare for assertions stage.
        """
        super().act()
        self.generic_syncer.report_errors()
