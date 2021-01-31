import datetime
import pickle

from collections import Counter

import mock
import pandas as pd
import pytest
import six

from dbnd import config, dbnd_tracking_stop, log_metric, task
from dbnd._core.configuration.environ_config import get_max_calls_per_func
from dbnd._core.constants import RunState, TaskRunState
from dbnd._core.errors import DatabandRunError
from dbnd._core.tracking.schemas.tracking_info_objects import (
    TaskDefinitionInfo,
    TaskRunInfo,
)
from dbnd._core.utils.timezone import utcnow
from dbnd.testing.helpers_mocks import set_airflow_context
from test_dbnd.conftest import set_tracking_context


@task
def task_pass_through_default(data, dt, expect_pass_through):
    # type: (pd.DataFrame, datetime.datetime, bool) -> str
    # print needed to test that log is sent
    print("hello task_pass_through_default")
    if expect_pass_through:
        assert isinstance(data, str)
        assert isinstance(dt, str)
    else:
        assert isinstance(data, pd.DataFrame)
        assert isinstance(dt, datetime.datetime)
    log_metric("data", data)
    return str(data)


@task
def task_pass_through_nested_default(data, dt, expect_pass_through):
    # type: (pd.DataFrame, datetime.datetime, bool) -> str
    # print needed to test that log is sent
    print("hello task_pass_through_nested_default")
    if expect_pass_through:
        assert isinstance(data, str)
        assert isinstance(dt, str)
    else:
        assert isinstance(data, pd.DataFrame)
        assert isinstance(dt, datetime.datetime)
    res = task_pass_through_default(data, dt, expect_pass_through)
    return str(res) + str(res)


@task
def task_pass_through_result_param(result):
    assert isinstance(result, str)
    return str(result)


@task
def task_pass_through_exception():
    # print needed to test that log is sent
    print("hello task_pass_through_exception")
    1 / 0


@task
def task_pass_through_args_kwargs(*args, **kwargs):
    return {
        "args": args,
        "kwargs": kwargs,
    }


@task
def task_in_conf(param1="default_value", param2=None):
    return param1, param2


@pytest.fixture
def databand_context_kwargs():
    return dict(conf={"core": {"tracker": ["console", "debug"]}})


@pytest.fixture
def mock_channel_tracker():
    with mock.patch(
        "dbnd._core.tracking.backends.tracking_store_channels.TrackingStoreThroughChannel._m"
    ) as mock_store:
        yield mock_store


def _check_tracking_calls(mock_store, expected_tracking_calls_counter):
    actual_store_calls = Counter(
        [call.args[0].__name__ for call in mock_store.call_args_list]
    )
    # assert expected_tracking_calls_counter == actual_store_calls would also work, but this
    # will make it easier to compare visually
    assert sorted(actual_store_calls.items()) == sorted(
        expected_tracking_calls_counter.items()
    )


def test_pickle():
    pickled = pickle.dumps(task_pass_through_default)
    assert task_pass_through_default == pickle.loads(pickled)


@pytest.mark.usefixtures(set_airflow_context.__name__)
def test_tracking_pass_through_default_airflow(
    pandas_data_frame_on_disk, mock_channel_tracker
):
    df, df_file = pandas_data_frame_on_disk

    # we'll pass string instead of defined expected DataFrame and it should work
    from targets.values import DateTimeValueType

    some_date = DateTimeValueType().to_str(utcnow())
    task_result = task_pass_through_default(
        str(df_file), some_date, expect_pass_through=True
    )
    assert task_result == str(df_file)

    _check_tracking_calls(
        mock_channel_tracker,
        {
            "init_run": 1,
            "add_task_runs": 1,  # real task only
            "update_task_run_attempts": 2,  # DAG start(with task start), task finished
            "log_metrics": 1,
            "log_targets": 1,
            "save_task_run_log": 1,
        },
    )

    _assert_tracked_params(
        mock_channel_tracker,
        task_pass_through_default,
        data=str(df_file),
        dt=some_date,
        expect_pass_through=True,
    )

    # this should happen on process exit in normal circumstances
    dbnd_tracking_stop()

    _check_tracking_calls(
        mock_channel_tracker,
        {
            "init_run": 1,
            "add_task_runs": 1,
            "update_task_run_attempts": 3,  # as above +   airflow root stop
            "log_metrics": 1,
            "log_targets": 1,
            "set_run_state": 1,
            "save_task_run_log": 2,  # as above + airflow root log
        },
    )


@pytest.mark.usefixtures(set_tracking_context.__name__)
def test_tracking_pass_through_default_tracking(
    pandas_data_frame_on_disk, mock_channel_tracker
):
    df, df_file = pandas_data_frame_on_disk

    # we'll pass string instead of defined expected DataFrame and it should work
    some_date = utcnow().isoformat()
    task_result = task_pass_through_default(
        str(df_file), some_date, expect_pass_through=True
    )
    assert task_result == str(df_file)
    # this should happen on process exit in normal circumstances
    dbnd_tracking_stop()

    _check_tracking_calls(
        mock_channel_tracker,
        {
            "init_run": 1,
            "add_task_runs": 1,
            "log_metrics": 1,
            "log_targets": 1,
            "save_task_run_log": 2,
            "set_run_state": 1,
            "update_task_run_attempts": 3,  # DAG start(with task start), task finished, dag stop
        },
    )

    _assert_tracked_params(
        mock_channel_tracker,
        task_pass_through_default,
        data=str(df_file),
        dt=some_date,
        expect_pass_through=True,
    )


def _assert_tracked_params(mock_channel_tracker, task_func, **kwargs):
    tdi, tri = _get_tracked_task_run_info(mock_channel_tracker, task_func)
    tdi_params = {tpd.name: tpd for tpd in tdi.task_param_definitions}
    tri_params = {tp.parameter_name: tp for tp in tri.task_run_params}
    for name in kwargs.keys():
        assert name in tdi_params

    for k, v in six.iteritems(kwargs):
        assert tri_params[k].value == str(v)


def _get_tracked_task_run_info(mock_channel_tracker, task_cls):
    tdi_result, tri_result = None, None
    for call in mock_channel_tracker.call_args_list:
        if call.args[0].__name__ == "add_task_runs":
            for tdi in call.kwargs[
                "task_runs_info"
            ].task_definitions:  # type: TaskDefinitionInfo
                if tdi.name == task_cls.__name__:
                    tdi_result = tdi
            for tri in call.kwargs["task_runs_info"].task_runs:  # type: TaskRunInfo
                if tri.name == task_cls.__name__:
                    tri_result = tri

            if tdi_result and tri_result:
                return tdi_result, tri_result


@pytest.mark.usefixtures(set_airflow_context.__name__)
def test_tracking_pass_through_nested_default(
    pandas_data_frame_on_disk, mock_channel_tracker
):
    df, df_file = pandas_data_frame_on_disk

    # we'll pass string instead of defined expected DataFrame and it should work
    task_result = task_pass_through_nested_default(
        str(df_file), utcnow().isoformat(), expect_pass_through=True
    )
    assert task_result == str(df_file) + str(df_file)

    _check_tracking_calls(
        mock_channel_tracker,
        {
            "init_run": 1,
            "add_task_runs": 2,
            "update_task_run_attempts": 4,
            "log_metrics": 1,
            "log_targets": 2,
            "save_task_run_log": 2,
        },
    )

    # this should happen on process exit in normal circumstances
    dbnd_tracking_stop()

    _check_tracking_calls(
        mock_channel_tracker,
        {
            "init_run": 1,
            "add_task_runs": 2,
            "update_task_run_attempts": 5,
            "log_metrics": 1,
            "log_targets": 2,
            "set_run_state": 1,
            "save_task_run_log": 3,
        },
    )


@pytest.mark.usefixtures(set_airflow_context.__name__)
def test_tracking_user_exception(mock_channel_tracker):
    # we'll pass string instead of defined expected DataFrame and it should work
    with pytest.raises(ZeroDivisionError):
        task_pass_through_exception()

    _check_tracking_calls(
        mock_channel_tracker,
        {
            "init_run": 1,
            "add_task_runs": 1,
            "update_task_run_attempts": 2,
            "save_task_run_log": 1,
        },
    )

    # this should happen on process exit in normal circumstances
    dbnd_tracking_stop()

    _check_tracking_calls(
        mock_channel_tracker,
        {
            "init_run": 1,
            "add_task_runs": 1,
            "update_task_run_attempts": 3,
            "set_run_state": 1,
            "save_task_run_log": 2,
        },
    )

    update_task_run_attempts_chain = [
        call.kwargs["task_run_attempt_updates"][0].state
        for call in mock_channel_tracker.call_args_list
        if call.args[0].__name__ == "update_task_run_attempts"
    ]
    assert [
        TaskRunState.RUNNING,  # DAG
        TaskRunState.FAILED,  # task
        TaskRunState.UPSTREAM_FAILED,  # DAG
    ] == update_task_run_attempts_chain

    set_run_state_chain = [
        call.kwargs["state"]
        for call in mock_channel_tracker.call_args_list
        if call.args[0].__name__ == "set_run_state"
    ]
    assert [RunState.FAILED] == set_run_state_chain


@pytest.mark.usefixtures(set_airflow_context.__name__)
def test_tracking_pass_through_result_param(pandas_data_frame_on_disk):
    df, df_file = pandas_data_frame_on_disk

    assert task_pass_through_result_param(result=str(df_file)) == str(df_file)


@pytest.mark.usefixtures(set_airflow_context.__name__)
def test_tracking_pass_through_args_kwargs(pandas_data_frame_on_disk):
    df, df_file = pandas_data_frame_on_disk

    res = task_pass_through_args_kwargs(str(df_file), data=df, result=str(df_file))
    assert res["args"] == (str(df_file),)
    assert len(res["kwargs"]) == 2
    assert res["kwargs"]["data"] is df
    assert res["kwargs"]["result"] == str(df_file)


@pytest.mark.usefixtures(set_airflow_context.__name__)
def test_partial_params():
    param1, param2 = task_in_conf(param2="param2_value")
    assert param1 == "default_value"
    assert param2 == "param2_value"


@pytest.mark.usefixtures(set_airflow_context.__name__)
def test_task_in_conf():
    # in_conf - shouldn't affect anything
    with config(
        {"task_in_conf": {"param1": "conf_value", "param2": "conf_value"}},
        source="test_source",
    ):
        param1, param2 = task_in_conf(param2="param2_value")
        assert param1 == "default_value"
        assert param2 == "param2_value"


def test_dbnd_pass_through_default(pandas_data_frame_on_disk, mock_channel_tracker):
    df, df_file = pandas_data_frame_on_disk
    some_date = utcnow().isoformat()
    r = task_pass_through_default.dbnd_run(
        str(df_file), some_date, expect_pass_through=False
    )
    assert r.root_task.result.read() == str(df)

    _check_tracking_calls(
        mock_channel_tracker,
        {
            "init_run": 1,
            "add_task_runs": 1,
            "update_task_run_attempts": 4,  # DAG start, driver start, task start, task finished
            "log_metrics": 3,  # 1 data metric call, 2 marshalling data calls
            "log_targets": 2,  # read input "data" dataframe, write result
            "save_task_run_log": 2,  # task, driver
            "set_run_state": 2,  # running, success
        },
    )

    _assert_tracked_params(
        mock_channel_tracker,
        task_pass_through_default,
        data=str(
            df_file
        ),  # param value is file path, the DF value will be logged as log_targets
        dt=some_date.replace("+00:00", "").replace(":", ""),
        expect_pass_through=False,
    )


def test_dbnd_exception(mock_channel_tracker):
    # we'll pass string instead of defined expected DataFrame and it should work
    with pytest.raises(DatabandRunError):
        task_pass_through_exception.dbnd_run()

    _check_tracking_calls(
        mock_channel_tracker,
        Counter(
            {
                "init_run": 1,
                "set_run_state": 2,
                "update_task_run_attempts": 4,
                "add_task_runs": 1,
                "save_task_run_log": 2,
                "set_unfinished_tasks_state": 1,
            }
        ),
    )


@pytest.mark.usefixtures(set_airflow_context.__name__)
def test_tracking_limit(mock_channel_tracker):
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
