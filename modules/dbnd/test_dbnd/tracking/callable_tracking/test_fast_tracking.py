# © Copyright Databand.ai, an IBM Company 2022

import datetime

from collections import Counter

import pandas as pd
import pytest
import six

from dbnd import config, dbnd_tracking_stop, log_metric, task
from dbnd._core.constants import TaskRunState
from dbnd._core.errors import DatabandRunError
from dbnd._core.tracking.schemas.tracking_info_objects import (
    TaskDefinitionInfo,
    TaskRunInfo,
)
from dbnd._core.utils.timezone import utcnow
from dbnd.testing.helpers_mocks import set_airflow_context, set_tracking_context
from test_dbnd.tracking.tracking_helpers import get_call_args


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
def task_pass_through_exception():
    # print needed to test that log is sent
    print("hello task_pass_through_exception")
    1 / 0


@task
def task_pass_through_keyboard_interrupt():
    # print needed to test that log is sent
    raise KeyboardInterrupt


def _assert_tracked_params(mock_channel_tracker, task_func, **kwargs):
    tdi, tri = _get_tracked_task_run_info(mock_channel_tracker, task_func)
    tdi_params = {tpd.name: tpd for tpd in tdi.task_param_definitions}
    tri_params = {tp.parameter_name: tp for tp in tri.task_run_params}
    for name in kwargs:
        assert name in tdi_params

    for k, v in six.iteritems(kwargs):
        assert tri_params[k].value == str(v)


def _get_tracked_task_run_info(mock_channel_tracker, task_cls):
    tdi_result, tri_result = None, None
    for _, data in get_call_args(mock_channel_tracker, ["add_task_runs"]):
        for tdi in data["task_runs_info"].task_definitions:  # type: TaskDefinitionInfo
            if tdi.name == task_cls.__name__:
                tdi_result = tdi
        for tri in data["task_runs_info"].task_runs:  # type: TaskRunInfo
            if tri.name == task_cls.__name__:
                tri_result = tri

        if tdi_result and tri_result:
            return tdi_result, tri_result


def _check_tracking_calls(mock_store, expected_tracking_calls_counter):
    actual_store_calls = Counter([call.args[0] for call in mock_store.call_args_list])
    # assert expected_tracking_calls_counter == actual_store_calls would also work, but this
    # will make it easier to compare visually
    assert sorted(actual_store_calls.items()) == sorted(
        expected_tracking_calls_counter.items()
    )


@pytest.fixture
def tracking_config():
    with config({"tracking": {"capture_tracking_log": True}}):
        yield


@pytest.mark.usefixtures(set_airflow_context.__name__)
@pytest.mark.usefixtures(tracking_config.__name__)
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
            "update_task_run_attempts": 3,  # DAG start(with task start), task started running, task finished
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
            "update_task_run_attempts": 4,  # as above +   airflow root stop
            "log_metrics": 1,
            "log_targets": 1,
            "save_task_run_log": 2,  # as above + airflow root log
        },
    )


@pytest.mark.usefixtures(tracking_config.__name__)
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
            "update_task_run_attempts": 4,  # DAG start(with task start), task started running, task finished, dag stop
        },
    )

    _assert_tracked_params(
        mock_channel_tracker,
        task_pass_through_default,
        data=str(df_file),
        dt=some_date,
        expect_pass_through=True,
    )


@pytest.mark.usefixtures(tracking_config.__name__)
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
            "update_task_run_attempts": 5,
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
            "update_task_run_attempts": 6,
            "log_metrics": 1,
            "log_targets": 2,
            "save_task_run_log": 3,
        },
    )


@pytest.mark.usefixtures(tracking_config.__name__)
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
            "update_task_run_attempts": 3,
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
            "update_task_run_attempts": 4,
            "save_task_run_log": 2,
        },
    )

    update_task_run_attempts_chain = [
        data["task_run_attempt_updates"][0].state
        for _, data in get_call_args(mock_channel_tracker, ["update_task_run_attempts"])
    ]
    assert [
        TaskRunState.RUNNING.name,  # DAG
        TaskRunState.RUNNING.name,  # root_task
        TaskRunState.FAILED.name,  # task
        TaskRunState.UPSTREAM_FAILED.name,  # DAG
    ] == update_task_run_attempts_chain


@pytest.mark.usefixtures(tracking_config.__name__)
@pytest.mark.usefixtures(set_airflow_context.__name__)
def test_tracking_keyboard_interrupt(mock_channel_tracker):
    # we'll pass string instead of defined expected DataFrame and it should work
    try:
        task_pass_through_keyboard_interrupt()
    except BaseException:
        _check_tracking_calls(
            mock_channel_tracker,
            {
                "init_run": 1,
                "add_task_runs": 1,
                "update_task_run_attempts": 3,
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
                "update_task_run_attempts": 4,
                "save_task_run_log": 2,
            },
        )

        update_task_run_attempts_chain = [
            data["task_run_attempt_updates"][0].state
            for _, data in get_call_args(
                mock_channel_tracker, ["update_task_run_attempts"]
            )
        ]
        assert [
            TaskRunState.RUNNING.name,  # DAG
            TaskRunState.RUNNING.name,  # root_task
            TaskRunState.FAILED.name,  # task
            TaskRunState.UPSTREAM_FAILED.name,  # DAG
        ] == update_task_run_attempts_chain


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
