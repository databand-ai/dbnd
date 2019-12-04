""" Parameters are one of the core concepts of databand.
All Parameters sit on :class:`~dbnd.tasks.Task` classes.
See :ref:`parameter` for more info on how to define parameters.
"""
from __future__ import absolute_import

from typing import Dict, List, Set, Tuple

from dbnd import output, parameter
from dbnd._core.parameter.value_types.task_value import TaskValueType
from targets.values import (
    DateTimeValueType,
    DateValueType,
    ObjectValueType,
    TargetPathValueType,
    TargetValueType,
    TimeDeltaValueType,
)
from targets.values.custom_datetime_values import (
    DateHourValueType,
    DateMinuteValueType,
    DateSecondValueType,
    MonthValueType,
    YearValueType,
)
from targets.values.numpy_values import NumpyArrayValueType
from targets.values.pandas_values import DataFramesDictValueType, DataFrameValueType
from targets.values.timedelta_value import DateIntervalValueType


##############################
# BACKWARD COMPATABILITY
##############################


Parameter = parameter.type(ObjectValueType)

StrParameter = parameter[str]
IntParameter = parameter[int]
FloatParameter = parameter[float]
BoolParameter = parameter[bool]

DictParameter = parameter[Dict]
ListParameter = parameter[List]
SetParameter = parameter[Set]
FileListParameter = parameter[List[str]]
ListStrParameter = parameter[List[str]]
TupleParameter = parameter[Tuple]

# target parameters
TargetParameter = parameter.type(TargetValueType)
TargetPathParameter = parameter.type(TargetPathValueType)
# date time parameters
DateTimeParameter = parameter.type(DateTimeValueType)
DateParameter = parameter.type(DateValueType)
DateIntervalParameter = parameter.type(DateIntervalValueType)
TimeDeltaParameter = parameter.type(TimeDeltaValueType)

DateHourParameter = parameter.type(DateHourValueType)
DateMinuteParameter = parameter.type(DateMinuteValueType)
DateSecondParameter = parameter.type(DateSecondValueType)

MonthParameter = parameter.type(MonthValueType)
YearParameter = parameter.type(YearValueType)

# special parameters
TaskParameter = parameter.type(TaskValueType)

# pandas
DataFrameParameter = parameter.type(DataFrameValueType)
DataFramesDictParameter = parameter.type(DataFramesDictValueType)

NumpyArrayParameter = parameter.type(NumpyArrayValueType)

# backward compatibility
# please don't use it,
# use TargetParameter, TargetPathParameter or DataFrameParameter
TaskInput = parameter
TaskOutput = output
##############################
