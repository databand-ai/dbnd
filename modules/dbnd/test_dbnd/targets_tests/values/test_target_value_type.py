from dbnd._core.utils import json_utils
from dbnd._vendor import fast_hasher
from targets import target
from targets.target_meta import TargetMeta
from targets.types import Path
from targets.values import TargetPathLibValueType


class TestTargetValueType(object):
    def test_parse_from_str(self):
        obj = TargetPathLibValueType()
        v = target("a")
        assert isinstance(obj.target_to_value(v), Path)

    def test_target_value_meta(self):
        v = target("a")
        target_value_meta = TargetPathLibValueType().get_value_meta(v)

        expected_value_meta = TargetMeta(
            value_preview='"a"',
            data_dimensions=None,
            data_schema=json_utils.dumps({"type": "Path"}),
            data_hash=fast_hasher.hash(v),
        )

        assert target_value_meta == expected_value_meta
