# © Copyright Databand.ai, an IBM Company 2022
from __future__ import absolute_import

from typing import TypeVar, Union

from dbnd import dbnd_run_cmd, output, parameter
from dbnd._core.run.databand_run import DatabandRun
from dbnd_run.task.python_task import PythonTask


T = TypeVar("T")


def run_locally__raises(expected_exception, args, conf=None):
    import pytest

    with pytest.raises(expected_exception):
        dbnd_run_cmd(args)


def assert_run_task(task):  # type: (Union[T, Task]) -> Union[T, Task]
    task.dbnd_run()
    assert task._complete()
    return task


def assert_run(task):  # type: (Union[T, Task]) -> DatabandRun
    run = task.dbnd_run()
    assert task._complete()
    return run


class TTask(PythonTask):
    t_param = parameter.value("1")
    t_output = output.data

    def run(self):
        self.t_output.write("%s" % self.t_param)
