# © Copyright Databand.ai, an IBM Company 2022


from dbnd import parameter
from targets.values import ValueTypeLoader, register_value_type


class TestParameterValue(object):
    def test_value_type_list(self):
        register_value_type(
            ValueTypeLoader(
                "test_dbnd.parameters.lazy_types_examples.lazy_type_simple.LazyTypeForTest1",
                "test_dbnd.parameters.lazy_types_examples.lazy_type_simple.LazyTypeForTest1_ValueType",
                "dbnd-core",
            )
        )

        from test_dbnd.parameters.lazy_types_examples.lazy_type_simple import (
            LazyTypeForTest1,
            LazyTypeForTest1_ValueType,
        )

        p_def = parameter[LazyTypeForTest1].parameter
        assert isinstance(p_def.value_type, ValueTypeLoader)

        # parameter after build
        p = parameter[LazyTypeForTest1]._p
        assert isinstance(p.value_type, LazyTypeForTest1_ValueType)
