from collections import defaultdict
from typing import Any, Dict

import six

import attr

from targets import InMemoryTarget, Target
from targets.values import ValueType


@attr.s
class ValueOrigin(object):
    obj_id = attr.ib()  # type: int  # object id()
    origin_target = attr.ib()  # type: Target
    value_type = attr.ib()  # type: ValueType


class TargetIdentitySourceMap(object):
    """
    Used to store map from target's value object (id()) to target it came from (+ parameter).
    Usually will be stored on DatabandRun level.
    When using dynamic (inline) tasks - used to know origin target some value came from.
    """

    def __init__(self):
        self.id_map = defaultdict(list)

    def add(self, origin_target, runtime_value, value_type):
        # type: (Target, Any, ValueType) -> None
        # https://stackoverflow.com/questions/306313/is-operator-behaves-unexpectedly-with-integers
        # or should we just exclude all integers? what about other types? like:
        # http://guilload.com/python-string-interning/
        if (
            runtime_value is None
            or isinstance(runtime_value, six.integer_types)
            or not isinstance(origin_target, Target)
        ):
            return

        obj_id = id(runtime_value)
        if isinstance(origin_target, InMemoryTarget):
            value_type = origin_target.value_type
        self.id_map[obj_id].append(
            ValueOrigin(
                obj_id=obj_id, origin_target=origin_target, value_type=value_type
            )
        )

    def get(self, v):
        # type: (Any) -> ValueOrigin
        return self.id_map.get(id(v))

    def get_for_map(self, d):
        # type: (Dict[str, Any]) -> Dict[str, ValueOrigin]
        return {k: self.id_map[id(v)][0] for k, v in d.items() if id(v) in self.id_map}
