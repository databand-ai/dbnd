from typing import Any, Tuple

import pytest

from dbnd import dbnd_config, task
from dbnd._core.constants import RESULT_PARAM
from dbnd.testing.helpers_mocks import set_tracking_context
from test_dbnd.tracking.tracking_helpers import (
    get_reported_params,
    get_task_multi_target_result,
    get_task_target_result,
)


@pytest.mark.usefixtures(set_tracking_context.__name__)
class TestReportParams(object):
    @pytest.mark.parametrize(
        "log_value_preview, expected_input_param, expected_inputs_args, expected_inputs_kwargs, expected_result_value_preview",
        [
            [True, "a", "[1,2,3,4,5,5]", '{"b":20,"others":123}', "6"],
            [False, None, None, None, None],
        ],
    )
    def test_decorated_report_params(
        self,
        log_value_preview,
        expected_input_param,
        expected_inputs_args,
        expected_inputs_kwargs,
        expected_result_value_preview,
        mock_channel_tracker,
    ):
        @task()
        def my_task(a, *args, **kwargs):
            return 6

        # executing the task
        with dbnd_config(
            config_values={"tracking": {"log_value_preview": log_value_preview}}
        ):
            my_task("a", 1, 2, 3, 4, 5, 5, b=20, others=123)

        # get the parameters reported to the tracker
        # we want to compare that for each parameter value we have a definition
        # otherwise the webserver wouldn't have all the needed information
        param_definitions, run_time_params, _ = get_reported_params(
            mock_channel_tracker, "my_task"
        )
        assert set(param_definitions) == set(run_time_params)

        # we want to be sure that the right parameter values where reported
        assert run_time_params["args"].value == expected_inputs_args
        assert run_time_params["kwargs"].value == expected_inputs_kwargs
        assert run_time_params["a"].value == expected_input_param

        # we want to check that we report the result target correctly
        result_target_info = get_task_target_result(mock_channel_tracker, "my_task")
        if log_value_preview:
            expected_target_path = result_target_info.target_path
        else:
            expected_target_path = None
        assert run_time_params[RESULT_PARAM].value == expected_target_path


@pytest.mark.usefixtures(set_tracking_context.__name__)
class TestReportResults(object):
    def test_result_none(self, mock_channel_tracker):
        @task()
        def my_task(a, b):
            # type: (int, int) -> None
            return None

        # executing the task
        my_task(1, 2)

        param_definitions, run_time_params, _ = get_reported_params(
            mock_channel_tracker, "my_task"
        )

        # no reporting for None result
        assert RESULT_PARAM not in run_time_params
        assert RESULT_PARAM not in param_definitions

        # no target for None result
        assert get_task_target_result(mock_channel_tracker, "my_task") is None

    def test_result_any(self, mock_channel_tracker):
        @task()
        def my_task(a, b):
            # type: (int, int) -> Any
            return 1

        # executing the task
        my_task(1, 2)

        param_definitions, run_time_params, _ = get_reported_params(
            mock_channel_tracker, "my_task"
        )

        # single parameter default named result, should be both in parameter definitions and run_time_params
        assert RESULT_PARAM in run_time_params
        assert RESULT_PARAM in param_definitions

        # check that a target was reported
        result_target_info = get_task_target_result(mock_channel_tracker, "my_task")
        assert result_target_info is not None

        # the target_path of the target is the value of the run_time_param
        assert run_time_params[RESULT_PARAM].value == result_target_info.target_path
        # check tracking the value
        assert result_target_info.value_preview == "1"
        # check tracking the schema
        assert result_target_info.data_schema == '{"type": "int"}'

    def test_result_multiple_values(self, mock_channel_tracker):
        @task(result=("first", "second"))
        def my_task(a, b):
            # type: (int, int) -> Tuple[Any,Any]
            return 1, 2

        # executing the task
        my_task(1, 2)

        (
            param_definitions,
            run_time_params,
            param_def_task_def_uid,
        ) = get_reported_params(mock_channel_tracker, "my_task")

        # reporting the definition of the result proxy
        assert RESULT_PARAM in param_definitions

        # 2 inner parameters of the proxy
        result_params = [
            name
            for name, p in param_definitions.items()
            if name != RESULT_PARAM and p.kind == "task_output"
        ]
        assert len(result_params) == 2
        for name in result_params:
            # the inner parameters values reported on runtime
            assert name in run_time_params

        # multiple targets reported
        targets_info = get_task_multi_target_result(
            mock_channel_tracker, "my_task", ("first", "second")
        )

        # for each - the value of the param is the path of the target
        assert run_time_params["first"].value == targets_info["first"].target_path
        assert targets_info["first"].value_preview == "1"
        assert targets_info["first"].task_def_uid is not None
        assert targets_info["first"].task_def_uid == param_def_task_def_uid["first"]

        assert run_time_params["second"].value == targets_info["second"].target_path
        assert targets_info["second"].value_preview == "2"
        assert targets_info["second"].task_def_uid is not None
        assert targets_info["second"].task_def_uid == param_def_task_def_uid["second"]

    def test_result_multiple_values_disabled(self, mock_channel_tracker):
        @task()
        def my_task(a, b):
            # type: (int, int) -> Any
            return 1

        # executing the task
        with dbnd_config(config_values={"tracking": {"log_value_preview": False}}):
            my_task(1, 2)

        param_definitions, run_time_params, _ = get_reported_params(
            mock_channel_tracker, "my_task"
        )

        # reporting the definition of the result proxy
        assert run_time_params == {
            "a": {
                "parameter_name": "a",
                "value": None,
                "value_origin": "t.t.c.t.my_task",
            },
            "b": {
                "parameter_name": "b",
                "value": None,
                "value_origin": "t.t.c.t.my_task",
            },
            "result": {
                "parameter_name": "result",
                "value": None,
                "value_origin": "t.t.c.t.my_task",
            },
        }
