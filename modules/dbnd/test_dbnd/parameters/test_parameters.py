# © Copyright Databand.ai, an IBM Company 2022

import enum

from typing import List, Tuple

import pytest

from dbnd import parameter
from targets.types import NullableStr


class MyEnum(enum.Enum):
    A = 1


class TestParameters(object):
    def test_enum_param_valid(self):
        p = parameter.enum(MyEnum)._p
        assert MyEnum.A == p.parse_from_str("A")

    def test_enum_param_invalid(self):
        p = parameter.enum(MyEnum)._p
        with pytest.raises(ValueError):
            p.parse_from_str("B")

    def test_list_serialize_parse(self):
        a = parameter[List[int]]._p
        b_list = [1, 2, 3]
        assert b_list == a.parse_from_str(a.to_str(b_list))

    def test_tuple_serialize_parse(self):
        a = parameter[Tuple]._p
        b_tuple = ((1, 2), (3, 4))
        assert b_tuple == a.parse_from_str(a.to_str(b_tuple))

    def test_optional_parameter_parse_none(self):
        assert parameter[NullableStr]._p.parse_from_str("") is None

    def test_optional_parameter_parse_string(self):
        assert "test" == parameter[NullableStr]._p.parse_from_str("test")

    def test_optional_parameter_serialize_none(self):
        assert "" == parameter[NullableStr]._p.to_str(None)

    def test_optional_parameter_serialize_string(self):
        assert "test" == parameter[NullableStr]._p.to_str("test")
