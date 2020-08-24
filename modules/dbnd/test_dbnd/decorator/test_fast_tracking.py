import datetime

from collections import Counter

import mock
import pandas as pd
import pytest

from dbnd import config, dbnd_run_stop, log_metric, task
from dbnd._core.configuration.environ_config import (
    get_max_calls_per_func,
    reset_dbnd_project_config,
)
from dbnd._core.constants import RunState, TaskRunState
from dbnd._core.errors import DatabandRunError
from dbnd._core.inplace_run.airflow_dag_inplace_tracking import AirflowTaskContext
from dbnd._core.utils.timezone import utcnow


COMPOSITE_TRACKING_STORE_INVOKE_REF = "dbnd._core.tracking.backends.tracking_store_composite.CompositeTrackingStore._invoke"


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
def set_af_context():
    with mock.patch(
        "dbnd._core.inplace_run.airflow_dag_inplace_tracking.try_get_airflow_context"
    ) as m:
        try:
            reset_dbnd_project_config()

            m.return_value = AirflowTaskContext(
                dag_id="test_dag",
                task_id="test_task",
                execution_date=utcnow().isoformat(),
            )
            yield
        finally:
            # ensure dbnd_run_stop() is called (normally should happen on exit() )
            dbnd_run_stop()
            reset_dbnd_project_config()


def _check_tracking_calls(mock_store, tracking_calls_counter):
    store_calls = Counter([call.args[0] for call in mock_store.call_args_list])
    # assert tracking_calls_counter == store_calls would also work, but this
    # will make it easier to compare visually
    assert sorted(tracking_calls_counter.items()) == sorted(store_calls.items())


@pytest.mark.usefixtures("set_af_context")
def test_tracking_pass_through_default(pandas_data_frame_on_disk):
    df, df_file = pandas_data_frame_on_disk

    with mock.patch(COMPOSITE_TRACKING_STORE_INVOKE_REF) as mock_store:
        # we'll pass string instead of defined expected DataFrame and it should work
        task_result = task_pass_through_default(
            str(df_file), utcnow().isoformat(), expect_pass_through=True
        )
        assert task_result == str(df_file)

        _check_tracking_calls(
            mock_store,
            {
                "init_run": 1,
                "add_task_runs": 2,
                "set_task_run_state": 4,  # DAG start, driver start, task start, task finished
                "log_metrics": 1,
                "log_target": 1,
                "save_task_run_log": 1,
            },
        )

        # this should happen on process exit in normal circumstances
        dbnd_run_stop()

        _check_tracking_calls(
            mock_store,
            {
                "init_run": 1,
                "add_task_runs": 2,
                "set_task_run_state": 6,  # as above + driver stop, DAG stop
                "log_metrics": 1,
                "log_target": 1,
                "set_run_state": 1,
                "save_task_run_log": 3,  # as above + drive log, DAG log
            },
        )


@pytest.mark.usefixtures("set_af_context")
def test_tracking_pass_through_nested_default(pandas_data_frame_on_disk):
    df, df_file = pandas_data_frame_on_disk

    with mock.patch(COMPOSITE_TRACKING_STORE_INVOKE_REF) as mock_store:
        # we'll pass string instead of defined expected DataFrame and it should work
        task_result = task_pass_through_nested_default(
            str(df_file), utcnow().isoformat(), expect_pass_through=True
        )
        assert task_result == str(df_file) + str(df_file)

        _check_tracking_calls(
            mock_store,
            {
                "init_run": 1,
                "add_task_runs": 3,
                "set_task_run_state": 6,
                "log_metrics": 1,
                "log_target": 2,
                "save_task_run_log": 2,
            },
        )

        # this should happen on process exit in normal circumstances
        dbnd_run_stop()

        _check_tracking_calls(
            mock_store,
            {
                "init_run": 1,
                "add_task_runs": 3,
                "set_task_run_state": 8,
                "log_metrics": 1,
                "log_target": 2,
                "set_run_state": 1,
                "save_task_run_log": 4,
            },
        )


@pytest.mark.usefixtures("set_af_context")
def test_tracking_user_exception():
    with mock.patch(COMPOSITE_TRACKING_STORE_INVOKE_REF) as mock_store:
        # we'll pass string instead of defined expected DataFrame and it should work
        with pytest.raises(ZeroDivisionError):
            task_pass_through_exception()

        _check_tracking_calls(
            mock_store,
            {
                "init_run": 1,
                "add_task_runs": 2,
                "set_task_run_state": 4,
                "save_task_run_log": 1,
            },
        )

        # this should happen on process exit in normal circumstances
        dbnd_run_stop()

        _check_tracking_calls(
            mock_store,
            {
                "init_run": 1,
                "add_task_runs": 2,
                "set_task_run_state": 6,
                "set_run_state": 1,
                "save_task_run_log": 3,
            },
        )

        set_task_run_state_chain = [
            call.args[1]["state"]
            for call in mock_store.call_args_list
            if call.args[0] == "set_task_run_state"
        ]
        assert [
            TaskRunState.RUNNING,  # driver
            TaskRunState.RUNNING,  # DAG
            TaskRunState.RUNNING,  # task
            TaskRunState.FAILED,  # task
            TaskRunState.UPSTREAM_FAILED,  # DAG
            TaskRunState.SUCCESS,  # driver
        ] == set_task_run_state_chain

        set_run_state_chain = [
            call.args[1]["state"]
            for call in mock_store.call_args_list
            if call.args[0] == "set_run_state"
        ]
        assert [RunState.FAILED] == set_run_state_chain


@pytest.mark.usefixtures("set_af_context")
def test_tracking_pass_through_result_param(pandas_data_frame_on_disk):
    df, df_file = pandas_data_frame_on_disk

    assert task_pass_through_result_param(result=str(df_file)) == str(df_file)


@pytest.mark.usefixtures("set_af_context")
def test_tracking_pass_through_args_kwargs(pandas_data_frame_on_disk):
    df, df_file = pandas_data_frame_on_disk

    res = task_pass_through_args_kwargs(str(df_file), data=df, result=str(df_file))
    assert res["args"] == (str(df_file),)
    assert len(res["kwargs"]) == 2
    assert res["kwargs"]["data"] is df
    assert res["kwargs"]["result"] == str(df_file)


@pytest.mark.usefixtures("set_af_context")
def test_partial_params():
    param1, param2 = task_in_conf(param2="param2_value")
    assert param1 == "default_value"
    assert param2 == "param2_value"


@pytest.mark.usefixtures("set_af_context")
def test_task_in_conf():
    # in_conf - shouldn't affect anything
    with config(
        {"task_in_conf": {"param1": "conf_value", "param2": "conf_value"}},
        source="test_source",
    ):
        param1, param2 = task_in_conf(param2="param2_value")
        assert param1 == "default_value"
        assert param2 == "param2_value"


def test_dbnd_pass_through_default(pandas_data_frame_on_disk):
    df, df_file = pandas_data_frame_on_disk
    r = task_pass_through_default.dbnd_run(
        str(df_file), utcnow().isoformat(), expect_pass_through=False
    )
    assert r.root_task.result.read() == str(df)


def test_dbnd_excetion():
    with mock.patch(COMPOSITE_TRACKING_STORE_INVOKE_REF) as mock_store:
        # we'll pass string instead of defined expected DataFrame and it should work
        with pytest.raises(DatabandRunError):
            task_pass_through_exception.dbnd_run()

        _check_tracking_calls(
            mock_store,
            Counter(
                {
                    "init_run": 1,
                    "set_run_state": 2,
                    "set_task_run_state": 4,
                    "add_task_runs": 1,
                    "save_task_run_log": 2,
                    "set_unfinished_tasks_state": 1,
                }
            ),
        )


@pytest.mark.usefixtures("set_af_context")
def test_tracking_limit():
    @task
    def inc_task(x):
        return x + 1

    max_calls_allowed = get_max_calls_per_func()
    extra_func_calls = 10

    with mock.patch(COMPOSITE_TRACKING_STORE_INVOKE_REF) as mock_store:
        n = 0
        for i in range(max_calls_allowed + extra_func_calls):
            n = inc_task(n)

        # ensure that function was actually invoked all the times (max_calls_allowed + extra_func_calls)
        assert max_calls_allowed + extra_func_calls == n

        # check that there was only max_calls_allowed "tracked" calls
        track_call = [x for x in mock_store.call_args_list if x.args[0] == "log_target"]
        assert max_calls_allowed == len(track_call)
