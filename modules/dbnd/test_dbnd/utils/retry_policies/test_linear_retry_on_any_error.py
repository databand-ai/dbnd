# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd._core.utils.http.retry_policy import LinearRetryOnAnyError


SECONDS_TO_SLEEP = 2.0
MAX_RETRIES = 5
VALID_STATUS_CODE = 700
INVALID_STATUS_CODE = 200
RETRY_ERROR = Exception()


@pytest.fixture
def retry_policy():
    return LinearRetryOnAnyError(SECONDS_TO_SLEEP, MAX_RETRIES)


def test_retry_on_valid_status_and_raised_error_code(retry_policy):
    assert retry_policy.should_retry(
        status_code=VALID_STATUS_CODE, error=RETRY_ERROR, retry_count=MAX_RETRIES - 1
    )


def test_retry_on_valid_status_and_no_raised_error_code(retry_policy):
    assert retry_policy.should_retry(
        status_code=VALID_STATUS_CODE, error=None, retry_count=MAX_RETRIES - 1
    )


def test_retry_on_invalid_status_and_no_raised_error_code(retry_policy):
    assert retry_policy.should_retry(
        status_code=INVALID_STATUS_CODE, error=None, retry_count=MAX_RETRIES - 1
    )


def test_retry_on_invalid_status_and_raised_error_code(retry_policy):
    assert retry_policy.should_retry(
        status_code=INVALID_STATUS_CODE, error=RETRY_ERROR, retry_count=MAX_RETRIES - 1
    )


def test_not_retry_on_valid_status_and_raised_error_code(retry_policy):
    assert not retry_policy.should_retry(
        status_code=VALID_STATUS_CODE, error=RETRY_ERROR, retry_count=MAX_RETRIES
    )


def test_not_retry_on_valid_status_and_no_raised_error_code(retry_policy):
    assert not retry_policy.should_retry(
        status_code=VALID_STATUS_CODE, error=None, retry_count=MAX_RETRIES
    )


def test_not_retry_on_invalid_status_and_no_raised_error_code(retry_policy):
    assert not retry_policy.should_retry(
        status_code=INVALID_STATUS_CODE, error=None, retry_count=MAX_RETRIES
    )


def test_not_retry_on_invalid_status_and_raised_error_code(retry_policy):
    assert not retry_policy.should_retry(
        status_code=INVALID_STATUS_CODE, error=RETRY_ERROR, retry_count=MAX_RETRIES
    )


def test_seconds_to_sleep(retry_policy):
    assert retry_policy.seconds_to_sleep(MAX_RETRIES - 1) == SECONDS_TO_SLEEP
