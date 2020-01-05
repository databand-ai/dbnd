import datetime

from typing import List

import pytest

from pandas import DataFrame
from pandas.util.testing import assert_frame_equal

from dbnd import output, task
from dbnd._core.errors import DatabandBuildError, DatabandExecutorError
from dbnd.testing.helpers_pytest import assert_run_task
from test_dbnd.targets_tests import TargetTestBase


class TestTaskDecoReturnValues(TargetTestBase):
    def test_return_complex_result(self, pandas_data_frame):
        my_target = self.target("file.parquet")
        my_target.write_df(pandas_data_frame)

        now = datetime.datetime.now()

        @task
        def t_f(a_str, b_datetime, c_timedelta, d_int):
            # type: (str, datetime.datetime, datetime.timedelta, int) -> (DataFrame, DataFrame)
            assert a_str == "strX"
            assert b_datetime == now
            assert c_timedelta == datetime.timedelta(seconds=1)
            assert d_int == 1
            return pandas_data_frame, pandas_data_frame

        args = ["strX", now, datetime.timedelta(seconds=1), 1]
        func_res1, func_res2 = t_f(*args)
        assert_frame_equal(pandas_data_frame, func_res1)
        assert_frame_equal(pandas_data_frame, func_res2)
        t = assert_run_task(t_f.t(*args))

        task_res1, task_res2 = t.result
        assert_frame_equal(pandas_data_frame, task_res1.load(DataFrame))
        assert_frame_equal(pandas_data_frame, task_res2.load(DataFrame))

    def test_return_tuple(self, pandas_data_frame):
        my_target = self.target("file.parquet")
        my_target.write_df(pandas_data_frame)

        now = datetime.datetime.now()

        @task(
            result=(output(name="task_res1").parquet, output.parquet(name="task_res2"))
        )
        def t_f(a_str, b_datetime, c_timedelta, d_int):
            # type: (str, datetime.datetime, datetime.timedelta, int) -> (DataFrame, DataFrame)
            assert a_str == "strX"
            assert b_datetime == now
            assert c_timedelta == datetime.timedelta(seconds=1)
            assert d_int == 1
            return pandas_data_frame, pandas_data_frame

        args = ["strX", now, datetime.timedelta(seconds=1), 1]
        func_res1, func_res2 = t_f(*args)
        assert_frame_equal(pandas_data_frame, func_res1)
        assert_frame_equal(pandas_data_frame, func_res2)
        t = assert_run_task(t_f.t(*args))

        task_res1, task_res2 = t.result
        assert_frame_equal(pandas_data_frame, task_res1.read_df())
        assert_frame_equal(pandas_data_frame, task_res2.read_df())
        assert_frame_equal(pandas_data_frame, t.task_res1.read_df())
        assert_frame_equal(pandas_data_frame, t.task_res2.read_df())

    def test_ret_value(self, pandas_data_frame):
        my_target = self.target("file.parquet")
        my_target.write_df(pandas_data_frame)

        @task
        def t_f():
            return pandas_data_frame

        assert_frame_equal(pandas_data_frame, t_f())
        t = assert_run_task(t_f.t())
        assert_frame_equal(pandas_data_frame, t.result.read_df())

    def test_ret_dict(self):
        @task(result=(output(name="o_a").csv[List[str]], "o_b"))
        def t_f(a=5):
            return {"o_a": [str(a)], "o_b": ["2"]}

        t = assert_run_task(t_f.t(a=6))
        assert "6\n" == t.o_a.read()

    def test_ret_defined_via_string(self):
        @task(result="o_a,o_b")
        def t_f_str(a=5):
            return {"o_a": [str(a)], "o_b": ["2"]}

        t = t_f_str.dbnd_run(a=6).root_task
        assert "6\n" == t.o_a.read()

    def test_type_in_deco(self):
        @task(result=int)
        def t_f_in_deco(same_name=3, a=5):
            return 4

        t = assert_run_task(t_f_in_deco.t())
        assert 4 == t.result.load(int)

    def test_no_result(self):
        @task(result=None)
        def t_f_no_result(same_name=3, a=5):
            return None

        t = assert_run_task(t_f_no_result.t())
        assert t.result is None

    def test_fails_on_same_names(self):
        with pytest.raises(
            DatabandBuildError, message="have same keys in result schema"
        ):

            @task(result=(output(name="same_name").csv[List[str]], "o_b"))
            def t_f(same_name=3, a=5):
                return {"o_a": [str(a)], "o_b": same_name}

    def test_fails_on_dict_spec(self):
        with pytest.raises(
            DatabandBuildError, message="have same keys in result schema"
        ):

            @task(result={"a": List[str]})
            def t_f(same_name=3, a=5):
                return {"o_a": [str(a)], "o_b": same_name}

    def test_fails_on_wrong_ret_type(self):
        @task
        def t_f():
            # type: ()->(str, str)
            return ""

        with pytest.raises(
            DatabandExecutorError, message="have same keys in result schema"
        ):
            t_f.dbnd_run()

    def test_fails_on_wrong_ret_len(self):
        @task
        def t_f():
            # type: ()->(str, str)
            return ("",)

        with pytest.raises(
            DatabandExecutorError, message="have same keys in result schema"
        ):
            t_f.dbnd_run()

    def test_one_result(self):
        @task(result="my_result,")
        def t_f():
            # type: ()->(str)
            return ""

        t_f.dbnd_run()

        t = t_f.dbnd_run().task
        assert hasattr(t, "result")

    def test_none_result(self):
        @task
        def t_f_none():
            # type: ()->None
            pass

        t = t_f_none.dbnd_run().root_task
        assert t.result is None
