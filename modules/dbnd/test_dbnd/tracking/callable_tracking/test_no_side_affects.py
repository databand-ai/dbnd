import pickle

from pytest import fixture

from dbnd import config, task
from dbnd._core.configuration.environ_config import get_max_calls_per_func


@task
def task_pass_through_result_param(result):
    assert isinstance(result, str)
    return str(result)


@task
def task_pass_through_args_kwargs(*args, **kwargs):
    return {"args": args, "kwargs": kwargs}


@task
def task_in_conf(param1="default_value", param2=None):
    return param1, param2


class TestNoSideAffectsOnTracking(object):
    @fixture(autouse=True)
    def _tracking_context(self, set_tracking_context):
        pass

    def test_tracking_pass_through_result_param(self, pandas_data_frame_on_disk):
        df, df_file = pandas_data_frame_on_disk

        assert task_pass_through_result_param(result=str(df_file)) == str(df_file)

    def test_tracking_pass_through_args_kwargs(self, pandas_data_frame_on_disk):
        df, df_file = pandas_data_frame_on_disk

        res = task_pass_through_args_kwargs(str(df_file), data=df, result=str(df_file))
        assert res["args"] == (str(df_file),)
        assert len(res["kwargs"]) == 2
        assert res["kwargs"]["data"] is df
        assert res["kwargs"]["result"] == str(df_file)

    def test_partial_params(self):
        param1, param2 = task_in_conf(param2="param2_value")
        assert param1 == "default_value"
        assert param2 == "param2_value"

    def test_task_in_conf(self):
        # in_conf - shouldn't affect anything
        with config(
            {"task_in_conf": {"param1": "conf_value", "param2": "conf_value"}},
            source="test_source",
        ):
            param1, param2 = task_in_conf(param2="param2_value")
            assert param1 == "default_value"
            assert param2 == "param2_value"

    def test_pickle(self):
        pickled = pickle.dumps(task_pass_through_args_kwargs)
        assert task_pass_through_args_kwargs == pickle.loads(pickled)

    def test_tracking_limit(self, mock_channel_tracker):
        @task
        def inc_task(x):
            return x + 1

        max_calls_allowed = get_max_calls_per_func()
        extra_func_calls = 10

        n = 0
        for i in range(max_calls_allowed + extra_func_calls):
            n = inc_task(n)

        # ensure that function was actually invoked all the times (max_calls_allowed + extra_func_calls)
        assert max_calls_allowed + extra_func_calls == n

        # check that there was only max_calls_allowed "tracked" calls
        track_call = [
            x
            for x in mock_channel_tracker.call_args_list
            if x.args[0].__name__ == "log_targets"
        ]
        assert max_calls_allowed == len(track_call)
