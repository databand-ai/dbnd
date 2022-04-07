import mock
import pytest

from mock.mock import Mock

from dbnd._core.constants import TaskRunState
from dbnd._core.tracking.backends import ConsoleStore


@pytest.fixture
def mock_logger():
    with mock.patch(
        "dbnd._core.tracking.backends.tracking_store_console.logger"
    ) as mock_logger:
        yield mock_logger


def test_af_tracking_mode_tracker_url_logging(mock_logger):
    store = ConsoleStore()
    store._is_in_airflow_tracking_mode = True
    task_run = Mock()
    task_run.task.task_name = "test_task"
    task_run.task_tracker_url = "http://example.com/app/jobs/test_task/..."

    store.set_task_run_state(task_run, state=TaskRunState.RUNNING)

    mock_logger.info.assert_called_once_with(
        "Tracking %s task at %s",
        "test_task",
        "http://example.com/app/jobs/test_task/...",
    )
