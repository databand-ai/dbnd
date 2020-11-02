import logging
import typing

from dbnd._core.errors.errors_utils import log_exception
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
from targets.values.value_type import InlineValueType
from targets.values.version_value import VersionValueType


if typing.TYPE_CHECKING:
    from typing import Any, Optional
    from targets.value_meta import ValueMetaConf, ValueMeta


# Note: order matters. Examples:
# isinstance(True, int) == True, so it's important to have bool check before int
# isinstance(datetime.datetime.utc(), date) == True
if typing.TYPE_CHECKING:
    from typing import Optional, Any
    from targets.value_meta import ValueMeta, ValueMetaConf

logger = logging.getLogger(__name__)


known_values = []
try:
    import matplotlib
    from targets.values.matplotlib_values import MatplotlibFigureValueType

    known_values.append(MatplotlibFigureValueType())
except ImportError:
    pass

try:
    import pandas
    import numpy
    from targets.values.pandas_values import (
        DataFrameValueType,
        PandasSeriesValueType,
        DataFramesDictValueType,
    )

    known_values.append(DataFrameValueType())
    known_values.append(PandasSeriesValueType())
    known_values.append(DataFramesDictValueType())
except ImportError:
    pass

try:
    import numpy
    from targets.values.numpy_values import NumpyArrayValueType

    known_values.append(NumpyArrayValueType())
except ImportError:
    pass

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


def get_value_meta_from_value(
    value_name, value, meta_conf, raise_on_error=False
):  # type: (str, Any, ValueMetaConf, bool) -> Optional[ValueMeta]
    if value is None:
        return None

    obj_value_type = get_value_type_of_obj(value, default_value_type=ObjectValueType())
    if obj_value_type is None:
        logger.info(
            "Can't detect known type for '%s' with type='%s' ", value_name, type(value)
        )
        return None
    try:
        return obj_value_type.get_value_meta(value, meta_conf=meta_conf)

    except Exception as ex:
        log_exception(
            "Failed to get value meta info for '%s' with type='%s'"
            " ( detected as %s)" % (value_name, type(value), obj_value_type),
            ex,
            non_critical=not raise_on_error,
        )
        if raise_on_error:
            raise
    return None
