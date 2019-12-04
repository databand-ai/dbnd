import pytest

from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.utils import json_utils
from targets.values import InlineValueType, StrValueType, register_value_type


class TestValueType(object):
    def test_parse_from_str(self):
        v = StrValueType().parse_from_str("a")
        assert v == "a"

    def test_dump_to_str(self):
        v = StrValueType().to_str("a")
        assert v == "a"

    def test_non_supported_parse_from_str(self):
        class MyTObj(object):
            pass

        actual = register_value_type(InlineValueType(MyTObj))

        with pytest.raises(DatabandRuntimeError):
            actual.parse_from_str("a")

    def test_data_dimensions(self):
        data_dimensions = StrValueType().get_data_dimensions("a")

        assert data_dimensions is None

    def test_data_schema(self):
        data_shape = StrValueType().get_data_schema("a")

        assert data_shape == json_utils.dumps({"type": "str"})
