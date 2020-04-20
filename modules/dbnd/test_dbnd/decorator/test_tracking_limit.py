import mock

from dbnd import task
from dbnd._core.configuration.environ_config import get_max_calls_per_func
from dbnd._core.decorator.decorated_task import _DecoratedTask


NUMBER_OF_EXTRA_FUNC_CALLS = 10


@task
def my_func():
    return 1


def test_tracking_limit():
    max_calls_allowed = get_max_calls_per_func()

    with mock.patch.object(_DecoratedTask, "_call_handler", autospec=True) as f:
        for i in range(max_calls_allowed + NUMBER_OF_EXTRA_FUNC_CALLS):
            my_func()

        assert f.call_count == max_calls_allowed
