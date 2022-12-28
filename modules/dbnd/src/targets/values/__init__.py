# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from targets.values.builtins_values import (
    DataValueType,
    IntValueType,
    ObjectValueType,
    StrValueType,
    ValueType,
)
from targets.values.datetime_value import DateTimeValueType, DateValueType
from targets.values.registry import ValueTypeRegistry
from targets.values.structure import (
    DictValueType,
    ListValueType,
    SetValueType,
    TupleValueType,
)
from targets.values.target_values import (
    TargetPathLibValueType,
    TargetPathValueType,
    TargetValueType,
)
from targets.values.timedelta_value import DateIntervalValueType, TimeDeltaValueType
from targets.values.value_type import InlineValueType  # noqa: F401
from targets.values.value_type_loader import ValueTypeLoader
from targets.values.version_value import VersionValueType


__all__ = [
    "get_types_registry",
    "get_value_type_of_obj",
    "get_value_type_of_type",
    "ObjectValueType",
    "register_value_type",
    "InlineValueType",
    "IntValueType",
    "ValueType",
    "StrValueType",
    "DataValueType",
    "DictValueType",
    "ListValueType",
    "SetValueType",
    "TupleValueType",
    "ValueTypeLoader",
    "VersionValueType",
    "DateTimeValueType",
    "DateValueType",
    "DateIntervalValueType",
    "TimeDeltaValueType",
    "TargetPathValueType",
    "TargetPathLibValueType",
    "TargetPathValueType",
    "TargetValueType",
]

if typing.TYPE_CHECKING:
    from typing import Optional

logger = logging.getLogger(__name__)

_VALUE_TYPES = ValueTypeRegistry()


def get_types_registry():
    # type: ()-> ValueTypeRegistry
    return _VALUE_TYPES


def get_value_type_of_obj(obj, default_value_type=None):
    # type: (object, Optional[ValueType]) -> Optional[ValueType]
    return get_types_registry().get_value_type_of_obj(obj, default_value_type)


def get_value_type_of_type(obj_type, inline_value_type=False):
    # we don't load value type here, as it might be used for obejct definition only
    return get_types_registry().get_value_type_of_type(
        obj_type, inline_value_type=inline_value_type
    )


def register_value_type(value_type):
    return get_types_registry().register_value_type(value_type)
