from typing import TypeVar, Union

import pytest

from dbnd import Task, dbnd_run_cmd
from dbnd._core.utils.platform import windows_compatible_mode


T = TypeVar("T")


def run_locally__raises(expected_exception, args, conf=None):
    import pytest

    with pytest.raises(expected_exception):
        dbnd_run_cmd(args)


def assert_run_task(task):  # type: (Union[T, Task]) -> Union[T, Task]
    task.dbnd_run()
    assert task._complete()
    return task


skip_on_windows = pytest.mark.skipif(
    windows_compatible_mode, reason="not supported on Windows OS"
)
