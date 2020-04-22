from dbnd._vendor import fast_hasher
from targets import target
from targets.types import Path
from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values import TargetPathLibValueType


class TestTargetValueType(object):
    def test_parse_from_str(self):
        obj = TargetPathLibValueType()
        v = target("a")
        assert isinstance(obj.target_to_value(v), Path)

    def test_target_value_meta(self):
        v = target("a")
        meta_conf = ValueMetaConf()
        target_value_meta = TargetPathLibValueType().get_value_meta(
            v, meta_conf=meta_conf
        )

        expected_value_meta = ValueMeta(
            value_preview='"a"',
            data_dimensions=None,
            data_schema={"type": "Path"},
            data_hash=fast_hasher.hash(v),
        )

        assert target_value_meta == expected_value_meta
