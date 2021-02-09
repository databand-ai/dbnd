from __future__ import absolute_import

import pytest
import six

from dbnd import parameter, task
from targets.values import StrValueType


if six.PY2:
    from future.builtins import *

    __future_module__ = True

py_2_only_import = pytest.importorskip("__builtin__")


@task
def task_with_str_param(something=parameter(default=None)[str]):
    return "aa"


def test_newstr_as_type():
    # future.builtins.str is actually "newstr",
    # we want to check that correct value type is selected
    assert "newstr" in repr(str)
    p = parameter(default=None)[str]
    assert isinstance(p.parameter.value_type, StrValueType)


def test_newstr_run():
    a = task_with_str_param.dbnd_run(something="333")
    print(a.root_task.something)
