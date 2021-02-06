import datetime
import json
import logging

from typing import List, Tuple

import pandas as pd

from pandas import DataFrame
from pandas.util.testing import assert_frame_equal
from pytest import fixture

from dbnd import band, data, dbnd_run_cmd, log_metric, output, task
from dbnd._core.task_build.task_context import current_task, try_get_current_task
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase
from targets.types import DataList
from targets.values import DictValueType, ListValueType
from targets.values.pandas_values import DataFrameValueType


logger = logging.getLogger(__name__)

_value_1_2 = ["1", "2"]


@task
def t_f_a(t_input, t_param, t_default="d1"):
    # type: (DataList[str], str, str) -> DataList[str]
    # adds dressing
    assert t_default == "d1"
    assert t_param == "d2"

    log_metric("t_input", len(t_input))

    logger.info("Got string: %s", t_input)
    return t_input[:2]


@task(result=output.data_list_str)
def t_f_b(t_input2):
    # type: (List[str]) -> List[str]

    logger.info("Got string: %s", t_input2)
    return ["s_%s" % s for s in t_input2]


@band(t_input=data)
def t_f_band(t_input, t_param="d2"):
    res1 = t_f_a(t_input, t_param)
    return t_f_b(res1)


class TestTaskDecorators(TargetTestBase):
    @fixture
    def target_1_2(self):
        t = self.target("file.txt")
        t.as_object.writelines(["1", "2"])
        return t

    def test_signature(self):
        @task
        def t_f(t_input, t_int, t_time):
            # type: (DataList[str], int, datetime.datetime) -> List[str]
            return ["1"]

        assert t_f.task.t_input.value_type.type == List
        assert t_f.task.t_int.value_type.type == int
        assert t_f.task.t_time.value_type.type == datetime.datetime
        assert t_f.task.result.value_type.type == List

    def test_t_f_band_native(self):
        value = t_f_band(t_input=_value_1_2)
        assert value == ["s_1", "s_2"]

    def test_t_f_band_dbnd(self, target_1_2):
        task = t_f_band.t(t_input=target_1_2)
        assert_run_task(task)
        assert ["s_1", "s_2"] == task.result.load(List[str])

    def test_t_f_a_dbnd(self, target_1_2):
        task = t_f_a.t(t_input=target_1_2, t_param="d2")
        assert_run_task(task)
        assert task.result.load(List[str]) == ["1", "2"]

    def test_type_hints_inline(self, pandas_data_frame):
        my_target = self.target("file.parquet")
        my_target.write_df(pandas_data_frame)

        now = datetime.datetime.now()

        @task
        def t_f_hints(a_str, b_datetime, c_timedelta, d_int):
            # type: (str, datetime.datetime, datetime.timedelta, int) -> DataFrame
            assert a_str == "strX"
            assert b_datetime == now
            assert c_timedelta == datetime.timedelta(seconds=1)
            assert d_int == 1
            return pandas_data_frame

        args = ["strX", now, datetime.timedelta(seconds=1), 1]
        assert_frame_equal(pandas_data_frame, t_f_hints(*args))
        t = assert_run_task(t_f_hints.t(*args))
        assert_frame_equal(pandas_data_frame, t.result.read_df())

    def test_type_hints_cmdline(self, pandas_data_frame):
        my_target = self.target("file.parquet")
        my_target.write_df(pandas_data_frame)

        @task
        def t_f_cmd_hints(a_str, b_datetime, c_timedelta, d_int):
            # type: (str, datetime.datetime, datetime.timedelta, int) -> DataFrame
            assert a_str == "1"
            assert b_datetime.isoformat() == "2018-01-01T10:10:10.100000+00:00"
            assert c_timedelta == datetime.timedelta(days=5)
            assert d_int == 1
            return pandas_data_frame

        dbnd_run_cmd(
            [
                "t_f_cmd_hints",
                "-r",
                "a_str=1",
                "-r",
                "b_datetime=2018-01-01T101010.1",
                "-r",
                "c_timedelta=5d",
                "-r",
                "d_int=1",
            ]
        )

    def test_type_hints_from_defaults_cmdline(self, pandas_data_frame):
        my_target = self.target("file.parquet")
        my_target.write_df(pandas_data_frame)

        @task
        def t_f_defaults_cmdline(
            a_str="",
            b_datetime=datetime.datetime.utcnow(),
            c_timedelta=datetime.timedelta(),
            d_int=0,
        ):
            assert a_str == "1"
            assert b_datetime.isoformat() == "2018-01-01T10:10:10.100000+00:00"
            assert c_timedelta == datetime.timedelta(days=5)
            assert d_int == 1
            return pandas_data_frame

        dbnd_run_cmd(
            [
                "t_f_defaults_cmdline",
                "-r",
                "a_str=1",
                "-r",
                "b_datetime=2018-01-01T101010.1",
                "-r",
                "c_timedelta=5d",
                "-r",
                "d_int=1",
            ]
        )

    def test_type_hints_output_simple(self, pandas_data_frame):
        @task
        def t_f_hints(a_str):
            # type: (str) -> Tuple[str, str]
            return "a", "b"

        a, b = t_f_hints.t("a").result
        assert a
        assert b

    def test_type_hints_py2(self):
        @task
        def t_f_hints(p_list, p_dict):
            # type: (list,dict) -> str
            assert isinstance(p_list, list)
            assert isinstance(p_dict, dict)
            return "a"

        a = t_f_hints.t(["a", "b"], {1: 1, 2: 2})
        logger.info(a.ctrl.banner(""))
        assert isinstance(a.__class__.p_list.value_type, ListValueType)
        assert isinstance(a.__class__.p_dict.value_type, DictValueType)

    def test_type_hints_output_df(self, pandas_data_frame):
        @task(result="a,b")
        def func_multiple_outputs():
            # type: ()-> (pd.DataFrame, pd.DataFrame)
            return (
                pd.DataFrame(data=[[1, 1]], columns=["c1", "c2"]),
                pd.DataFrame(data=[[1, 1]], columns=["c1", "c2"]),
            )

        t = func_multiple_outputs.task
        assert isinstance(t.a.value_type, DataFrameValueType)
        a, b = func_multiple_outputs.t().result
        assert a
        assert b

    def test_inline_override_in_decorator(self):
        @task(task_version="2")
        def my_task():
            assert current_task().task_version == "2"

        my_task.dbnd_run()

    def test_task_system_params_overrides_in_decorator(self):
        @task
        def my_task():
            assert current_task().task_name == "test_name"

        my_task.dbnd_run(task_name="test_name")
