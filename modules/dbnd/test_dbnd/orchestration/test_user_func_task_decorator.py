# © Copyright Databand.ai, an IBM Company 2022

import datetime
import json
import logging
import pickle

from typing import List, Optional, Tuple, Union

import numpy
import pandas as pd
import pytest

from numpy.testing import assert_array_equal
from pandas import DataFrame
from pandas.util.testing import assert_frame_equal

from dbnd import band, data, dbnd_run_cmd, log_metric, output, parameter, task
from dbnd._core.errors import DatabandBuildError
from dbnd._core.task_build.task_context import current_task
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase
from targets import FileTarget, Target
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


@task
def task_that_runs_inline(value=1.0):
    # type: (float)-> float
    return value + 0.1


@task
def task_that_spawns_inline_run(value=1.0):
    # type: (float)-> float

    value_task = task_that_runs_inline.task(value=value)
    value_task.dbnd_run()

    return value_task.result.read_pickle() + 0.1


class TestUserFuncTaskDecorator(TargetTestBase):
    def test_simple_defaults(self):
        @task
        def t_f_defaults(a=5):
            assert a == 5

        t_f_defaults()
        assert_run_task(t_f_defaults.t())

    def test_simple_no_call(self):
        @task
        def t_f_nocall(a=5):
            assert a == 6

        t_f_nocall(a=6)
        assert_run_task(t_f_nocall.t(a=6))

    def test_simple_with_call(self):
        @task()
        def t_f_call(a=5):
            assert a == 6

        t_f_call(a=6)
        assert_run_task(t_f_call.t(a=6))

    def test_definition_inplace_param(self):
        @task
        def t_f_call(a=parameter[int]):
            assert a == 6

        t_f_call(a=6)
        assert_run_task(t_f_call.t(a=6))

    def test_definition_inplace_output(self):
        @task
        def t_f_call(a=parameter[int], f_output=output[Target]):
            f_output.write(str(a))
            return None

        assert_run_task(t_f_call.t(a=6))

    def test_no_type(self):
        @task
        def err_f_no_type(a):
            return a

        err_f_no_type.dbnd_run(1)

    def test_wrong_type(self):
        @task
        def err_f_wrong_type(a):
            # type: () -> str
            return str(a)

        err_f_wrong_type.dbnd_run(1)

    def test_comment_first(self):
        @task
        def err_f_comment_first(a):
            # sss
            # type: (datetime.datetime)->str
            assert isinstance(a, datetime.datetime)
            return "OK"

        err_f_comment_first.dbnd_run("2018-01-01")

    def test_missing_params(self):
        @task
        def err_f_missing_params(a, b, c):
            # sss
            # type: (str) -> str
            return a

        err_f_missing_params.dbnd_run(1, 2, 3)

    def test_unknown_return_type(self):
        @task
        def err_f_unknown_return_type(a):
            # type: (str) -> err_f_missing_params
            return a

        err_f_unknown_return_type.dbnd_run(1)

    def test_double_definition(self):
        with pytest.raises(DatabandBuildError):

            @task(a=parameter[int])
            def err_f_unknown_return_type(a=parameter[str]):
                # type: (str) -> None
                return a

            err_f_unknown_return_type()  # ???

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

    def test_user_decorated_func_serializable(self):
        target_func = t_f_a
        pickled = pickle.dumps(target_func)
        assert target_func == pickle.loads(pickled)


class TestUserFuncInlineCalls(TargetTestBase):
    def test_inline_call_1(self, target_1_2):
        assert_run_task(task_that_spawns_inline_run.t())

    def test_inline_call_2(self, target_1_2):
        @task
        def t_f(a):
            assert a == ["1", "2"]

        @task
        def t_f_parent(a):
            # type: (DataList[str])-> None
            t_f(a)

        assert_run_task(t_f_parent.t(a=target_1_2))

    def test_inline_call_with_res(self, target_1_2):
        @task
        def t_f_parent(a):
            # type: (DataList[str])-> None
            x = t_f_b(a)
            assert x == ["s_1", "s_2"]

        assert_run_task(t_f_parent.t(a=target_1_2))

    def test_nested_inline_call(self, target_1_2):
        @task()
        def t_f_2nd(a):
            # type: (DataList[str])-> List[str]
            return t_f_b(a)

        @task
        def t_f_1st(a):
            # type: (DataList[str])-> None
            x = t_f_2nd(a)
            assert x == ["s_1", "s_2"]

        assert_run_task(t_f_1st.t(a=target_1_2))

    def test_inline_call_with_band(self, target_1_2):
        @task
        def t_f_2nd(a):
            # type: (DataList[str])-> List[str]
            return t_f_b(a)

        @task
        def t_f_1st(a):
            # type: (DataList[str])-> List[str]
            x = t_f_2nd(a)
            assert x == ["s_1", "s_2"]
            return x

        @band
        def t_f_band(a):
            # type: (DataList[str])-> FileTarget
            x = t_f_1st(a)
            assert isinstance(x, FileTarget)
            return x

        assert_run_task(t_f_band.t(a=target_1_2))

    def test_inline_call_with_inline_band(self, target_1_2):
        @task
        def t_f_2nd(a):
            # also, no typing
            return t_f_b(a)

        @band
        def t_f_inline_band(a):
            # also, no typing
            return t_f_2nd(a)

        @task
        def t_f_1st(a):
            # type: (DataList[str])-> List[str]
            x = t_f_inline_band(a)
            assert x == ["s_1", "s_2"]
            return x

        @band
        def t_f_band(a):
            # type: (DataList[str])-> FileTarget
            x = t_f_1st(a)
            assert isinstance(x, FileTarget)
            return x

        assert_run_task(t_f_band.t(a=target_1_2))

    def test_inline_call_df(self, pandas_data_frame):
        input_a = self.target("file.parquet")
        input_a.write_df(pandas_data_frame)
        calls = []

        @task
        def t_f_df(a):
            # type: (pd.DataFrame)-> pd.DataFrame
            calls.append(a)
            return a

        @task
        def t_f_parent(a):
            # type: (pd.DataFrame)-> pd.DataFrame
            x = t_f_df(a)
            y = t_f_df(a)

            assert_frame_equal(x, y)
            return y

        # test that task is executing once
        assert_run_task(t_f_parent.t(input_a))
        assert len(calls) == 1

    def test_inline_call_numpy(self, numpy_array):
        input_a = self.target("file.npy")
        input_a.write_numpy_array(numpy_array)

        calls = []

        @task
        def t_f_np(a):
            # type: (numpy.ndarray)-> numpy.ndarray
            calls.append(a)
            return a

        @task
        def t_f_parent(a):
            # type: (numpy.ndarray)-> numpy.ndarray
            x = t_f_np(a)
            y = t_f_np(a)
            assert_array_equal(x, y)
            return x

        # test that task is executing once
        assert_run_task(t_f_parent.t(input_a))

        assert len(calls) == 1


class TestFuncTypingAndHints(TargetTestBase):
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
                "--set",
                json.dumps({"t_f_defaults_cmdline": {"d_int": 1}}),
            ]
        )

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

    def test_simple_deco(self):
        @task
        def t_f_defaults(a=5):
            # type: (int)->str
            # some comment
            assert a == 5
            return "ok"

        assert t_f_defaults.task.a.value_type.type == int
        assert t_f_defaults.task.result.value_type.type == str

    def test_optional_deco(self):
        @task
        def t_f_defaults(a=5, b=2):
            # type: (Optional[int], Optional[str])->Optional[str]
            # some comment
            assert a == 5
            return "ok"

        assert t_f_defaults.task.a.value_type.type == int
        assert t_f_defaults.task.b.value_type.type == str
        assert t_f_defaults.task.result.value_type.type == str

    def test_optional_tuple(self):
        @task
        def t_f(a=5):
            # type: (int)->Tuple[Optional[str]]
            # some comment
            assert a == 5
            return ("ok",)

        assert t_f.task.a.value_type.type == int
        assert t_f.task.result_1.value_type.type == str

    def test_union(self):
        @task
        def t_f(a=5):
            # type: (Union[int, str])->Union[str]
            # some comment
            assert a == 5
            return "ok"

        assert t_f.task.a.value_type.type == int
        assert t_f.task.result.value_type.type == str
