import pytest

from dbnd._core.errors import DatabandRuntimeError
from dbnd._vendor import fast_hasher
from targets.value_meta import ValueMeta, ValueMetaConf
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
        str_value_meta = StrValueType().get_value_meta("foo", ValueMetaConf.enabled())
        expected_value_meta = ValueMeta(
            value_preview="foo",
            data_dimensions=None,
            data_schema={"type": "str"},
            data_hash=fast_hasher.hash("foo"),
            histogram_spec=str_value_meta.histogram_spec,
        )
        assert str_value_meta == expected_value_meta
