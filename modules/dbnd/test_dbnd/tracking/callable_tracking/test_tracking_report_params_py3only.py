import pytest

from dbnd import task
from dbnd.testing.helpers_mocks import set_tracking_context
from test_dbnd.tracking.tracking_helpers import get_log_targets, get_reported_params


@pytest.mark.usefixtures(set_tracking_context.__name__)
class TestReportParamsPY3(object):
    def test_decorated_report_params(self, mock_channel_tracker):
        @task()
        def my_task(a, *args, b=20, **kwargs):
            return 6

        # executing the task
        my_task("a", 1, 2, 3, 4, 5, 5, b=20, others=123)

        # get the parameters reported to the tracker
        # we want to compare that for each parameter value we have a definition
        # otherwise the webserver wouldn't have all the needed information
        param_definitions, run_time_params = get_reported_params(mock_channel_tracker)
        definitions_names = set(map(lambda p: p.name, param_definitions["my_task"]))
        params_names = set(map(lambda p: p.parameter_name, run_time_params["my_task"]))
        assert definitions_names == params_names

        # we want to be sure that the right parameter values where reported
        run_time_params = {p.parameter_name: p for p in run_time_params["my_task"]}
        assert run_time_params["args"].value == "[1,2,3,4,5,5]"
        assert run_time_params["kwargs"].value == '{"b":20,"others":123}'
        assert run_time_params["a"].value == "a"

        # we want to check that we report the result target correctly
        for target_info in get_log_targets(mock_channel_tracker):
            if (
                target_info.task_run_name == "my_task"
                and target_info.param_name == "result"
            ):
                assert run_time_params["result"].value == target_info.target_path
                assert target_info.value_preview == "6"
