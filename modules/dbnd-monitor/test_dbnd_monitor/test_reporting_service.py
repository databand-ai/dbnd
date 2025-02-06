# © Copyright Databand.ai, an IBM Company 2024

"""
Test scenario for error aggregation and reporting assumes that exceptions
could happen in initial cycle and could, but not necessarily, be raised
in following cycles. Depending on that ReportingService will send
different requests to the REST API endpoint with different payloads.
The base class Suite provides some auxiliary methods to drive a test,
the subsequent class in a hierarchy InitialCycleWithExceptions performs
the initial setup mocking some previously ended cycle, where it reports
exceptions supposedly happened while the cycle.
"""

from copy import copy
from random import shuffle
from time import sleep
from unittest.mock import MagicMock, call, patch

import pytest

from dbnd_monitor.error_handling.component_error import ComponentError
from dbnd_monitor.error_handling.error_aggregator import ComponentErrorAggregator
from dbnd_monitor.reporting_service import ReportingService


class Suite:
    """
    Provides auxiliary methods used in the test scenarios
    """

    integration_uid = "test_integration_uid"
    tracking_source_uid = "test_tracking_source_uid"
    external_id = "test_external_id"
    component = "TestGenericSyncer"

    expected_calls: list[
        list[ComponentError]
    ]  # Attention: initialize with empty list in the final child class

    reporting_service: ReportingService
    api_request: MagicMock

    initial_exceptions: list[Exception]

    @pytest.fixture
    def mock_reporting_service(self):
        with patch("dbnd.utils.api_client.ApiClient.api_request"):
            yield ReportingService("airflow", aggregator=ComponentErrorAggregator)

    @pytest.fixture
    def setup(self, mock_reporting_service):
        """
        The RepotingService instance with patched `api_request` method
        of encapsulated ApiClient instance to check REST API calls.
        """
        self.reporting_service = mock_reporting_service
        self.api_request = mock_reporting_service._api_client.api_request

    def assume_raised(self, exceptions: list[Exception] = None) -> None:
        """
        This auxiliary method feeds the aggregator with ComponentErrors
        based on the provided exceptions.
        Also it fills the `expected_calls` list with the errors we're
        expecting to see in the REST API request payload. This done for the
        sake of preserving the timestamp of ComponentErrors
        """

        def component_errors() -> list[ComponentError]:
            exs = copy(self.initial_exceptions if exceptions is None else exceptions)

            if len(exs) > 1:
                shuffle(exs)  # let's bring some uncertainty into the test

            result = []
            for ex in exs:
                result.append(ComponentError.from_exception(ex))
                sleep(0.1)  # let's there a timespan between consecutive exceptions
            return result

        comp_errors = component_errors()
        for comp_error in comp_errors:
            self.reporting_service.report_component_error(
                self.integration_uid, self.external_id, self.component, comp_error
            )  # reporting the ComponentError instance

        # in the payload ComponentErrors ordered from the latest to the earliest,
        # let's prepare for that
        self.expected_calls.append(comp_errors[::-1])

    @property
    def aggregator(self) -> ComponentErrorAggregator:
        """
        Returns the ComponentErrorAggregator instance holding reported
        ComponentError instances.
        """
        return self.reporting_service._error_aggregators[
            (self.integration_uid, self.external_id, self.component)
        ]

    @property
    def latest_api_call(self):
        """
        Returns the most recent call to the REST API endpoint made by
        ReportingService._api_client instance.
        """
        if self.api_request is not None and self.api_request.call_count > 0:
            return self.api_request.mock_calls[-1]
        return None


class InitialCycleWithExceptions(Suite):
    """
    Performs initial setup where the first cycle is completed with exceptions
    """

    initial_exceptions = [
        ValueError("function A"),
        AttributeError("object A"),
        AttributeError("object B"),
        ValueError("function B"),
    ]

    @pytest.fixture
    def initial_cycle(self, setup):
        # Assume some exceptions have been already caught
        self.assume_raised()

        # checking aggregator's state before calling `ReportingService.report_errors_dto` method
        assert len(self.aggregator.active_errors) == len(self.initial_exceptions)
        assert len(self.aggregator.errors) == 0

        # Act
        # The first cycle causes a REST API request should be done
        self.reporting_service.report_errors_dto(
            self.integration_uid, self.external_id, self.component
        )

        # checking aggregator's state after calling `ReportingService.report_errors_dto` method
        assert len(self.aggregator.active_errors) == 0
        assert len(self.aggregator.errors) == len(self.initial_exceptions)

        # at the moment there should be the only call to the API endpoint
        # with following call's parameters
        self.api_request.assert_called_once_with(
            endpoint="integrations/test_integration_uid/error?type=airflow",
            method="PATCH",
            data={
                "tracking_source_uid": "test_integration_uid",
                "external_id": "test_external_id",
                "component": "TestGenericSyncer",
                "errors": [error.dump() for error in self.expected_calls[0]],
                "is_error": True,
            },
        )

        yield


class TestReportingServiceSameRepeatingErrors(InitialCycleWithExceptions):
    """
    This test checks that if we have the same multiple errors for the component
    raised across consecutive cycles, then we don't report them again.
    """

    expected_calls = []

    @pytest.fixture
    def current_cycle(self, initial_cycle):
        # During the current cycle the same exceptions raised as during
        # the previous one
        self.assume_raised()

        # checking aggregator's state
        assert len(self.aggregator.active_errors) == len(self.initial_exceptions)
        assert len(self.aggregator.errors) == len(self.initial_exceptions)

        yield

    def test_report_component_error(self, current_cycle):
        # Act
        # ReportingService reports collected errors to the API
        self.reporting_service.report_errors_dto(
            self.integration_uid, self.external_id, self.component
        )

        # checking aggregator's state after calling `ReportingService.report_errors_dto` method
        assert len(self.aggregator.active_errors) == 0
        assert len(self.aggregator.errors) == len(self.initial_exceptions)

        # Assert
        # If the same set of exceptions was caught again,
        # there is nothing to report, `api_request` should not be called
        assert self.api_request.call_count == 1


class TestReportingServiceClearingErrors(InitialCycleWithExceptions):
    """
    This test checks that if no exceptions were raised in the cycle following
    after the such one, when some were raised, then we report clear data.
    """

    expected_calls = []

    @pytest.fixture
    def current_cycle(self, initial_cycle):
        # During the current cycle luckily no exceptions were raised
        self.assume_raised(exceptions=[])

        # checking aggregator's state
        assert len(self.aggregator.active_errors) == 0
        assert len(self.aggregator.errors) == len(self.initial_exceptions)

        yield

    def test_report_component_error(self, current_cycle):
        # Act
        # ReportingService reports collected errors to the API
        self.reporting_service.report_errors_dto(
            self.integration_uid, self.external_id, self.component
        )

        # Assert
        # After calling 'report_errors_dto' already registered ComponentError
        # instances being holded into `active_errors` go to `errors`
        # and `active_errors` is now cleaned
        assert len(self.aggregator.active_errors) == 0
        assert len(self.aggregator.errors) == 0

        # Since there was no exceptions in the current cycle,
        # ReportingService must send an empty list of errors to API,
        # so the number of calls increased by 1 and the last one must be
        # as follows
        assert self.api_request.call_count == 2

        assert self.latest_api_call == call(
            endpoint="integrations/test_integration_uid/error?type=airflow",
            method="PATCH",
            data={
                "tracking_source_uid": "test_integration_uid",
                "external_id": "test_external_id",
                "component": "TestGenericSyncer",
                "errors": [],
                "is_error": True,
            },
        )


class TestReportingServiceRepeatingPartOfErrors(InitialCycleWithExceptions):
    """
    This test checks that if some of already happened exceptions happened again
    and some did not, then we report only persisting ones.
    """

    current_exceptions = [AttributeError("object A"), AttributeError("object B")]

    expected_calls = []

    @pytest.fixture
    def current_cycle(self, initial_cycle):
        # During the current cycle the only following exceptions were reraised
        # ValueErrors reported earlier did not appear again

        self.assume_raised(self.current_exceptions)

        # checking aggregator's state
        assert len(self.aggregator.active_errors) == len(self.current_exceptions)
        assert len(self.aggregator.errors) == len(self.initial_exceptions)

        yield

    def test_report_component_error(self, current_cycle):
        # Act
        # ReportingService reports collected errors to the API
        self.reporting_service.report_errors_dto(
            self.integration_uid, self.external_id, self.component
        )

        # Assert
        # ReportingService sends a request to the API with a list of errors
        # happened during the current cycle, so the number of calls increased.
        # The payload contains only the errors, repored during the cycle.
        assert self.api_request.call_count == 2
        assert self.latest_api_call == call(
            endpoint="integrations/test_integration_uid/error?type=airflow",
            method="PATCH",
            data={
                "tracking_source_uid": "test_integration_uid",
                "external_id": "test_external_id",
                "component": "TestGenericSyncer",
                "errors": [error.dump() for error in self.expected_calls[-1]],
                "is_error": True,
            },
        )
