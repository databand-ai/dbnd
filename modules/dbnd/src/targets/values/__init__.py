# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from targets.values.builtins_values import (
    BoolValueType,
    CallableValueType,
    FloatValueType,
    IntValueType,
    NullableStrValueType,
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
from targets.values.target_config_value import TargetConfigValueType
from targets.values.target_values import (
    TargetPathLibValueType,
    TargetPathValueType,
    TargetValueType,
)
from targets.values.timedelta_value import DateIntervalValueType, TimeDeltaValueType
from targets.values.value_type import InlineValueType  # noqa: F401
from targets.values.version_value import VersionValueType


if typing.TYPE_CHECKING:
    from typing import Optional


logger = logging.getLogger(__name__)


known_values = []
try:
    import matplotlib  # noqa: F401

    from targets.values.matplotlib_values import MatplotlibFigureValueType

    known_values.append(MatplotlibFigureValueType())
except ImportError:
    pass

try:
    import numpy  # noqa: F401
    import pandas  # noqa: F401

    from targets.values.pandas_values import (
        DataFramesDictValueType,
        DataFrameValueType,
        PandasSeriesValueType,
    )

    known_values.append(DataFrameValueType())
    known_values.append(PandasSeriesValueType())
    known_values.append(DataFramesDictValueType())
except ImportError:
    pass

try:
    import numpy  # noqa: F401

    from targets.values.numpy_values import NumpyArrayValueType

    known_values.append(NumpyArrayValueType())
except ImportError:
    pass


# Note: order matters. Examples:
# isinstance(True, int) == True, so it's important to have bool check before int
# isinstance(datetime.datetime.utc(), date) == True
known_values.extend(
    [
        # simple
        BoolValueType(),
        IntValueType(),
        FloatValueType(),
        # date/time
        DateValueType(),
        DateTimeValueType(),
        TimeDeltaValueType(),
        DateIntervalValueType(),
        # structs,
        ListValueType(),
        DictValueType(),
        SetValueType(),
        TupleValueType(),
        # targets, path
        TargetValueType(),
        TargetPathValueType(),
        TargetPathLibValueType(),
        TargetConfigValueType(),
        CallableValueType(),
        # str types
        VersionValueType(),
        StrValueType(),
        NullableStrValueType(),
    ]
)


# OBJECT VALUE is always the last
known_values.append(ObjectValueType())

_VALUE_TYPES = ValueTypeRegistry(known_value_types=known_values)


def get_types_registry():
    # type: ()-> ValueTypeRegistry
    return _VALUE_TYPES


def get_value_type_of_obj(obj, default_value_type=None):
    # type: (object, Optional[ValueType]) -> Optional[ValueType]
    return get_types_registry().get_value_type_of_obj(obj, default_value_type)


def get_value_type_of_type(obj_type, inline_value_type=False):
    return get_types_registry().get_value_type_of_type(
        obj_type, inline_value_type=inline_value_type
    )


def register_value_type(value_type):
    return get_types_registry().register_value_type(value_type)
