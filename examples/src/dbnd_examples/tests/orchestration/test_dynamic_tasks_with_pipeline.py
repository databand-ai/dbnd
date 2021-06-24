import pytest
import six


if six.PY2:
    pytestmark = pytest.mark.skip("Python 2 non compatible code")  # py2 styling
else:
    from dbnd_examples.orchestration.dynamic_tasks_with_pipeline import (
        say_hello_to_everybody,
    )


class TestDynamicTasks(object):
    def test_run_say_hello_to_everybody(self):
        result = say_hello_to_everybody()
        assert (
            "Hey, user 2!",
            "Hey, some_user! and user 0 and user 1 and user 2",
        ) == result
