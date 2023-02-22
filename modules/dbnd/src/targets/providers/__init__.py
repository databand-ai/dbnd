# © Copyright Databand.ai, an IBM Company 2022

import typing

from targets.marshalling import register_marshallers
from targets.marshalling.file import (
    ObjJsonMarshaller,
    ObjPickleMarshaller,
    ObjYamlMarshaller,
    StrLinesMarshaller,
    StrMarshaller,
)
from targets.providers.matplotlib import register_value_types_matplotlib
from targets.providers.numpy import register_value_types_numpy
from targets.providers.pandas import register_value_type_pandas
from targets.providers.plugins import register_value_types_from_plugins
from targets.providers.spark import register_value_types_spark
from targets.target_config import FileFormat
from targets.types import DataList
from targets.values import register_value_type
from targets.values.builtins_values import (
    BoolValueType,
    CallableValueType,
    FloatValueType,
    IntValueType,
    NullableStrValueType,
    ObjectValueType,
    StrValueType,
)
from targets.values.datetime_value import DateTimeValueType, DateValueType
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
from targets.values.version_value import VersionStr, VersionValueType


def register_all_basic_types():

    ### Advanced TYPES
    register_value_type_pandas()
    register_value_types_matplotlib()
    register_value_types_numpy()
    register_value_types_spark()

    register_value_types_from_plugins()
    # register basic types as a fallback
    # Note: order matters. Examples:
    # isinstance(True, int) == True, so it's important to have bool check before int
    # isinstance(datetime.datetime.utc(), date) == True

    known_values = []

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

    for vt in known_values:
        register_value_type(vt)

    # this one will be used for a lot of default handling
    value_type_object = register_value_type(ObjectValueType())
    value_type_object.register_marshallers(
        {
            FileFormat.txt: StrMarshaller(),
            FileFormat.pickle: ObjPickleMarshaller(),
            FileFormat.json: ObjJsonMarshaller(),
            FileFormat.yaml: ObjYamlMarshaller(),
        }
    )

    list_kind_marshallers = {
        FileFormat.txt: StrLinesMarshaller(),
        FileFormat.csv: StrLinesMarshaller(),
        FileFormat.json: ObjJsonMarshaller(),
        FileFormat.yaml: ObjYamlMarshaller(),
        FileFormat.pickle: ObjPickleMarshaller(),
    }

    # typing.List[object], typing.List[str] is not required
    # as we can not handle the differnece now between List and List[whatever]
    for t in [typing.List, DataList]:
        register_marshallers(t, list_kind_marshallers)

    str_kind_marshallers = {
        FileFormat.txt: StrMarshaller(),
        FileFormat.csv: StrMarshaller(),
        FileFormat.pickle: ObjPickleMarshaller(),
        FileFormat.json: ObjJsonMarshaller(),
    }

    register_marshallers(str, str_kind_marshallers)
    register_marshallers(VersionStr, str_kind_marshallers)


register_all_basic_types()
