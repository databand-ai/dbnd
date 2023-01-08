# © Copyright Databand.ai, an IBM Company 2022


from dbnd import parameter
from targets.values import InlineValueType, ValueTypeLoader, register_value_type


class TypeWithValueTypeLoaderError(object):
    pass


class TestParameterValueTypeLoader(object):
    def test_value_type_loader(self):
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

    def test_value_type_loader_failed(self):
        c = TypeWithValueTypeLoaderError
        register_value_type(
            ValueTypeLoader(
                f"{c.__module__}.{c.__qualname__}",
                "test_dbnd.parameters.NotExists",
                "dbnd-core",
            )
        )

        p_def = parameter[TypeWithValueTypeLoaderError].parameter
        assert isinstance(p_def.value_type, ValueTypeLoader)

        # parameter after build
        p = parameter[TypeWithValueTypeLoaderError]._p
        assert isinstance(p.value_type, InlineValueType)
