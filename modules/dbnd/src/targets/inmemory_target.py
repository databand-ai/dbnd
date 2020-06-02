from typing import Any

from targets import Target
from targets.values import (
    ObjectValueType,
    ValueType,
    get_value_type_of_obj,
    get_value_type_of_type,
)


_NOTHING = object()


class InMemoryTarget(Target):
    def __init__(self, obj=_NOTHING, path=None, value_type=None, **kwargs):
        # type: (Any, str, ValueType, **Any) -> None
        from targets.values import get_value_type_of_obj

        super(InMemoryTarget, self).__init__(**kwargs)
        self._obj = obj
        if self._obj is _NOTHING and not path:
            raise Exception("InMemoryTarget requires object or path")

        self.value_type = value_type or get_value_type_of_obj(self._obj)
        self.path = path or "memory://%s:%s" % (
            self.value_type,
            self.value_type.to_signature(self._obj),
        )

    def exists(self):
        return self._obj is not _NOTHING

    def dump(self, value, value_type=None, **kwargs):
        self._obj = value
        if value_type:
            self.value_type = get_value_type_of_type(value_type)
        else:
            self.value_type = get_value_type_of_obj(value, ObjectValueType())

    def load(self, **kwargs):
        if not self.exists():
            raise ValueError("Value not assigned")
        return self._obj

    def __repr__(self):
        if isinstance(self._obj, Target):
            return str(self._obj)

        return self.path

    def __str__(self):
        return repr(self)
