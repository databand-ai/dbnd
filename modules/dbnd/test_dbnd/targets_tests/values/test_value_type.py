import pytest

from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.utils import json_utils
from dbnd._vendor import fast_hasher
from targets.target_meta import TargetMeta
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

    def test_str_value_meta(self):
        str_value_meta = StrValueType().get_value_meta("foo")
        expected_value_meta = TargetMeta(
            value_preview="foo",
            data_dimensions=None,
            data_schema=json_utils.dumps({"type": "str"}),
            data_hash=fast_hasher.hash("foo"),
        )
        assert str_value_meta == expected_value_meta
