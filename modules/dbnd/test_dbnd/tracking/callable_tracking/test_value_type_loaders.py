# © Copyright Databand.ai, an IBM Company 2022

from pytest import fixture

from dbnd import task
from targets.values import ValueTypeLoader, register_value_type
from test_dbnd.tracking.callable_tracking.lazy_value_types_examples import (
    LazyTypeForTrackingTest,
)
from test_dbnd.tracking.tracking_helpers import (
    get_reported_params,
    get_task_target_result,
)


@task
def task_with_loaded_param_loader(result: LazyTypeForTrackingTest):
    assert isinstance(result, LazyTypeForTrackingTest)
    return str(result)


class TypeWithValueTypeLoaderError(object):
    pass


@task
def task_with_loaded_param_error(
    result: TypeWithValueTypeLoaderError,
) -> TypeWithValueTypeLoaderError:
    assert isinstance(result, TypeWithValueTypeLoaderError)
    return result


class TestParameterValueTypeLoader(object):
    @fixture(autouse=True)
    def _tracking_context(self, set_tracking_context):
        pass

    def test_value_type_loader_with_tracking(self, mock_channel_tracker):
        register_value_type(
            ValueTypeLoader(
                "test_dbnd.tracking.callable_tracking.lazy_value_types_examples.LazyTypeForTrackingTest",
                "test_dbnd.tracking.callable_tracking.lazy_value_types_examples.lazy_type_loader_for_tracking_test.LazyTypeForTrackingTest_ValueType",
                "dbnd-core",
            )
        )
        task_with_loaded_param_loader(LazyTypeForTrackingTest())

        param_definitions, run_time_params, _ = get_reported_params(
            mock_channel_tracker, "task_with_loaded_param_loader"
        )

        result_target_info = get_task_target_result(
            mock_channel_tracker, "task_with_loaded_param_loader"
        )
        assert "object at" in result_target_info["value_preview"]

    def test_value_type_loader_failed(self, mock_channel_tracker):
        c = TypeWithValueTypeLoaderError
        register_value_type(
            ValueTypeLoader(
                f"{c.__module__}.{c.__qualname__}",
                "test_dbnd.parameters.NotExists",
                "dbnd-core",
            )
        )

        task_with_loaded_param_error(TypeWithValueTypeLoaderError())

        param_definitions, run_time_params, _ = get_reported_params(
            mock_channel_tracker, "task_with_loaded_param_error"
        )

        # reporting the definition of the result proxy
        assert "result" in run_time_params
