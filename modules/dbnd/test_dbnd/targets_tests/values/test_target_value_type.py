from targets import target
from targets.types import Path
from targets.values import TargetPathLibValueType


class TestTargetValueType(object):
    def test_parse_from_str(self):
        obj = TargetPathLibValueType()
        v = target("a")
        assert isinstance(obj.target_to_value(v), Path)
