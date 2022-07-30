# © Copyright Databand.ai, an IBM Company 2022

import pytest
import six

from dbnd._core.utils.type_check_utils import is_instance_by_class_name


class A(type):
    pass


@six.add_metaclass(A)
class B(object):
    pass


class C(B):
    pass


class D(C):
    pass


class E(object):
    pass


@pytest.mark.parametrize(
    "obj,name,expected",
    [
        (A("person", (object,), {"name": "kevin"}), "A", True),
        (B(), "A", False),
        (B(), "B", True),
        (C(), "B", True),
        (D(), "B", True),
        (D(), "B", True),
        (E(), "B", False),
    ],
)
def test_is_instance_by_class_name(obj, name, expected):
    assert is_instance_by_class_name(obj, name) == expected
