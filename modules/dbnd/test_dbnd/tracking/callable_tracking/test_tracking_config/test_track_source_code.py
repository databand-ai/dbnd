import pytest

from dbnd import config, task
from dbnd.testing.helpers_mocks import set_tracking_context
from test_dbnd.tracking.tracking_helpers import get_reported_source_code


@task
def enabled_task():
    # type: () -> bool
    return True


@task
def disabled_task():
    # type: () -> bool
    return True


@pytest.mark.usefixtures(set_tracking_context.__name__)
class TestTrackSourceCode(object):
    def test_track_source_code_enabled(self, mock_channel_tracker):
        with config({"tracking": {"track_source_code": True}}):
            enabled_task()  # run task
            reported_source_code = get_reported_source_code(mock_channel_tracker)
            assert reported_source_code

    def test_track_source_code_disabled(self, mock_channel_tracker):
        with config({"tracking": {"track_source_code": False}}):
            disabled_task()  # run task
            reported_source_code = get_reported_source_code(mock_channel_tracker)
            assert not reported_source_code
