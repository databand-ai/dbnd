import logging

from enum import Enum
from typing import Callable

import six

from dbnd._core.utils.basics.helpers import parse_bool
from dbnd._core.utils.basics.load_python_module import load_python_callable
from targets.types import NullableStr
from targets.values.value_type import ValueType


logger = logging.getLogger(__name__)

# custom types


# value types


class DataValueType(ValueType):
    """
      ValueType whose value is a ```target```.
      It will be "dereferenced accordingly to value_type
    """

    support_from_str = False
    load_on_build = False


class ObjectValueType(ValueType):
    type = object


class DefaultObjectValueType(ObjectValueType):
    """
    used by func decorators to mark that it's "auto" type
    """

    pass


class StrValueType(ValueType):
    type = str

    type_str_extras = ("unicode",)
    support_merge = True

    def merge_values(self, *values):
        return "".join(values)

    def is_type_of(self, value):
        return isinstance(value, six.string_types)

    def normalize(self, x):
        return str(x)


STR_VALUE_TYPE = StrValueType()


class NullableStrValueType(ValueType):
    """ A ValueType that treats empty string as None """

    type = NullableStr
    discoverable = False

    def to_str(self, x):
        if x is None:
            return ""
        else:
            return str(x)

    def parse_from_str(self, x):
        return x or None


class IntValueType(ValueType):
    type = int

    def parse_from_str(self, s):
        return int(s)

    def next_in_enumeration(self, value):
        return value + 1


class FloatValueType(ValueType):
    type = float

    def parse_from_str(self, s):
        """
        Parses a ``float`` from the string using ``float()``.
        """
        return float(s)


class EnumValueType(ValueType):
    type = Enum

    def __init__(self, enum):
        self._enum = enum
        super(EnumValueType, self).__init__()

    def parse_from_str(self, s):
        try:
            return self._enum[s]
        except KeyError:
            raise ValueError(
                "Invalid enum value '%s' - could not be parsed, use one of %s"
                % (s, list(self._enum))
            )

    def to_str(self, e):
        if e:
            return e.name
        return None


class BoolValueType(ValueType):
    type = bool

    def parse_from_str(self, s):
        return parse_bool(s)

    def normalize(self, value):
        return bool(value) if value is not None else None


class CallableValueType(ValueType):
    # should be used in configuration for now only
    type = Callable
    discoverable = False  # we can't really check if we got function

    def parse_from_str(self, s):
        return load_python_callable(callable_path=s)

    def to_str(self, x):
        return x.__name__
