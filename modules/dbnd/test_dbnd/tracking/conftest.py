import logging

import mock
import pytest

from dbnd._core.log import dbnd_log
from dbnd.testing.helpers_mocks import set_airflow_context, set_tracking_context


__all__ = ["set_tracking_context", "set_airflow_context"]


@pytest.fixture
def databand_context_kwargs():
    # we want extra tracking "debug" , so we can see all "tracking" calls on the screen
    return dict(conf={"core": {"tracker": ["console", "debug"]}})


@pytest.fixture
def mock_channel_tracker():
    with mock.patch(
        "dbnd._core.tracking.backends.channels.tracking_debug_channel.ConsoleDebugTrackingChannel._handle"
    ) as mock_store:
        yield mock_store


@pytest.fixture
def set_verbose_mode():
    logging.info("Setting verbose mode for unittest")
    origin_verbose = dbnd_log.is_verbose()
    try:
        dbnd_log.set_verbose()
        yield True
    finally:
        dbnd_log.set_verbose(origin_verbose)
