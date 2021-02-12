import mock
import pytest

from dbnd.testing.helpers_mocks import set_airflow_context, set_tracking_context


__all__ = ["set_tracking_context", "set_airflow_context"]


@pytest.fixture
def databand_context_kwargs():
    # we want extra tracking "debug" , so we can see all "tracking" calls on the screen
    return dict(conf={"core": {"tracker": ["console", "debug"]}})


@pytest.fixture
def mock_channel_tracker():
    with mock.patch(
        "dbnd._core.tracking.backends.tracking_store_channels.TrackingStoreThroughChannel._m"
    ) as mock_store:
        yield mock_store
