# originally from sparkmagic package
# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.s

from dbnd._core.errors import DatabandConfigError
from dbnd._core.utils.http.constants import (
    CONFIGURABLE_RETRY,
    LINEAR_RETRY,
    LINEAR_RETRY_ANY_ERROR,
)


# TODO implement real configuration
# implement configuration per name
conf_retry_seconds_to_sleep_list = [0.2, 0.5, 1, 3, 5]
conf_retry_policy_max_retries = 5


def get_retry_policy(name, policy=None, seconds_to_sleep=5, max_retries=5):
    # provide policy per name
    policy = policy or LINEAR_RETRY

    if policy == LINEAR_RETRY:
        return LinearRetryPolicy(
            seconds_to_sleep=seconds_to_sleep, max_retries=max_retries
        )
    elif policy == CONFIGURABLE_RETRY:
        return ConfigurableRetryPolicy(
            retry_seconds_to_sleep_list=conf_retry_seconds_to_sleep_list,
            max_retries=conf_retry_policy_max_retries,
        )
    elif policy == LINEAR_RETRY_ANY_ERROR:
        return LinearRetryOnAnyError(
            seconds_to_sleep=seconds_to_sleep, max_retries=max_retries
        )
    else:
        raise DatabandConfigError("Retry policy '{}' not supported".format(policy))


class LinearRetryPolicy(object):
    """Retry policy that always returns the same number of seconds to sleep between calls,
    takes all status codes 500 or above to be retriable, and retries a given maximum number of times."""

    def __init__(self, seconds_to_sleep, max_retries):
        self._seconds_to_sleep = seconds_to_sleep
        self.max_retries = max_retries

    def should_retry(self, status_code, error, retry_count):
        if None in (status_code, retry_count):
            return False
        if status_code < 500 or error:
            return False
        if self.max_retries != -1 and retry_count >= self.max_retries:
            return False
        return True

    def seconds_to_sleep(self, retry_count):
        return self._seconds_to_sleep


class ConfigurableRetryPolicy(LinearRetryPolicy):
    """Retry policy that returns a configurable number of seconds to sleep between calls,
    takes all status codes 500 or above to be retriable, and retries a given maximum number of times.
    If the retry count exceeds the number of items in the list, last item in the list is always returned."""

    def __init__(self, retry_seconds_to_sleep_list, max_retries):
        super(ConfigurableRetryPolicy, self).__init__(-1, max_retries)

        # If user configured to an empty list, let's make this behave as
        # a Linear Retry Policy by assigning a list of 1 element.
        if len(retry_seconds_to_sleep_list) == 0:
            retry_seconds_to_sleep_list = [5]
        elif not all(n > 0 for n in retry_seconds_to_sleep_list):
            raise DatabandConfigError(
                "All items in the list in your config need to be positive for configurable retry policy"
            )

        self.retry_seconds_to_sleep_list = retry_seconds_to_sleep_list
        self._max_index = len(self.retry_seconds_to_sleep_list) - 1

    def seconds_to_sleep(self, retry_count):
        index = max(retry_count - 1, 0)
        if index > self._max_index:
            index = self._max_index

        return self.retry_seconds_to_sleep_list[index]


class LinearRetryOnAnyError(LinearRetryPolicy):
    """Retry policy that always returns the same number of seconds to sleep between calls,
    takes all status codes to be retriable, and retries a given number of times."""

    def __init__(self, seconds_to_sleep, max_retries):
        super(LinearRetryOnAnyError, self).__init__(seconds_to_sleep, max_retries)

    def should_retry(self, status_code, error, retry_count):
        if self.max_retries != -1 and retry_count >= self.max_retries:
            return False
        return True
