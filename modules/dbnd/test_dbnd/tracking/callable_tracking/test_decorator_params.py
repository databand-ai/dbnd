# © Copyright Databand.ai, an IBM Company 2022

from typing import Any

import pytest

from dbnd import task
from dbnd._core.constants import RESULT_PARAM
from dbnd.testing.helpers_mocks import set_tracking_context
from test_dbnd.tracking.tracking_helpers import (
    get_reported_params,
    get_task_target_result,
)


@pytest.mark.usefixtures(set_tracking_context.__name__)
class TestReportResults(object):
    def test_task_family_name(self, mock_channel_tracker):
        @task(task_family="test_custom_func_name")
        def my_task(a, b):
            # type: (int, int) -> Any
            return 1

        # executing the task
        my_task(1, 2)

        param_definitions, run_time_params, _ = get_reported_params(
            mock_channel_tracker, "test_custom_func_name"
        )

        assert RESULT_PARAM in run_time_params
        assert RESULT_PARAM in param_definitions

        result_target_info = get_task_target_result(
            mock_channel_tracker, "test_custom_func_name"
        )
        assert result_target_info is not None

        assert run_time_params[RESULT_PARAM].value == result_target_info.target_path
        assert result_target_info.value_preview == "1"
        assert result_target_info.data_schema == '{"type": "int"}'
