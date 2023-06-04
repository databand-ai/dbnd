# © Copyright Databand.ai, an IBM Company 2022
from functools import partial

import pytest

from dbnd.utils.helpers import get_callable_name


def _foo(a):
    return a


add_part = partial(_foo, a=2)


@pytest.mark.parametrize(
    "input_class, expected_name", [(str, "str"), (partial, "partial")]
)
def test_get_object_class_name(input_class, expected_name):
    res = get_callable_name(input_class)
    assert res == expected_name


def test_get_object_partial_class_name():
    res = get_callable_name(add_part)
    assert res.split("(")[0] == "functools.partial"
