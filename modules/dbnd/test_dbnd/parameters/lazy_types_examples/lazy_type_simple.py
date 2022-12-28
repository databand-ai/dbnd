# © Copyright Databand.ai, an IBM Company 2022

from targets.values import ValueType


class LazyTypeForTest1(object):
    pass


class LazyTypeForTest1_ValueType(ValueType):
    type = LazyTypeForTest1
