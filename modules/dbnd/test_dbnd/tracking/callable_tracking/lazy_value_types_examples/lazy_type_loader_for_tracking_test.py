# © Copyright Databand.ai, an IBM Company 2022

from modules.dbnd.test_dbnd.tracking.callable_tracking.lazy_value_types_examples import (
    LazyTypeForTrackingTest,
)

from targets.values import ValueType


class LazyTypeForTrackingTest_ValueType(ValueType):
    type = LazyTypeForTrackingTest

    def __init__(self):
        super(LazyTypeForTrackingTest_ValueType, self).__init__()
