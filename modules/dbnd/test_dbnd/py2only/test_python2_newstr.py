from __future__ import absolute_import

import logging

import pytest
import six

from dbnd import parameter, task
from dbnd._core.current import try_get_current_task
from dbnd._core.task_ctrl.task_ctrl import TaskCtrl
from targets.values import StrValueType


if six.PY2:
    from future.builtins import *

    __future_module__ = True

py_2_only_import = pytest.importorskip("__builtin__")


@task
def task_with_str_param(something=parameter(default=None)[str]):
    current_task = try_get_current_task()
    ctrl = current_task.ctrl  # type: TaskCtrl
    task_as_cmd_line = ctrl.task_repr.calculate_command_line_for_task()
    logging.info("Str type: %s, task repr: %s", type(str), task_as_cmd_line)

    assert "newstr.BaseNewStr" in str(type(str))
    assert "@" not in task_as_cmd_line
    return "task_with_str"


def test_newstr_as_type():
    # future.builtins.str is actually "newstr",
    # we want to check that correct value type is selected
    assert "newstr" in repr(str)
    p = parameter(default=None)[str]
    assert isinstance(p.parameter.value_type, StrValueType)


def test_newstr_run():
    a = task_with_str_param.dbnd_run(something="333")
    print(a.root_task.something)
