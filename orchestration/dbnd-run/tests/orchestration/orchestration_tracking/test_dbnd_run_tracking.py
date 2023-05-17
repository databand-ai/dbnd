# © Copyright Databand.ai, an IBM Company 2022

import datetime

from collections import Counter
from typing import List

import pandas as pd
import pytest
import six

from mock import mock

from dbnd import config, log_metric, task
from dbnd._core.errors import DatabandRunError
from dbnd._core.tracking.schemas.tracking_info_objects import (
    TaskDefinitionInfo,
    TaskRunInfo,
)
from dbnd._core.utils.dotdict import rdotdict
from dbnd._core.utils.timezone import utcnow


def get_call_args(mock_channel_tracker, calls: List[str]):
    for call in mock_channel_tracker.call_args_list:
        if call.args[0] in calls:
            # we use rdotdict() here because many tests was written when it was possible
            # to use dot-notation to access data of channel-tracker calls, as data was
            # catched before convertion to json. This probably should be removed one day.
            yield call.args[0], rdotdict.try_create(call.args[1])


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
    with config(
        {
            "core": {"tracker": ["console", "debug"]},
            "tracking": {"capture_tracking_log": True},
        }
    ):
        yield


@pytest.fixture
def dbnd_config_for_test_run__user():
    # we want extra tracking "debug" , so we can see all "tracking" calls on the screen
    return {"core": {"tracker": ["console", "debug"]}}


@pytest.fixture
def mock_channel_tracker(tracking_config):
    with mock.patch(
        "dbnd._core.tracking.backends.channels.tracking_debug_channel.ConsoleDebugTrackingChannel._handle"
    ) as mock_store:
        yield mock_store


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
def task_pass_through_exception():
    # print needed to test that log is sent
    print("hello task_pass_through_exception")
    1 / 0


def test_dbnd_run_pass_through_default(pandas_data_frame_on_disk, mock_channel_tracker):
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


def test_dbnd_run_exception(mock_channel_tracker):
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
