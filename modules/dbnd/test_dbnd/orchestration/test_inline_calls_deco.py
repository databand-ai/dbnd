from typing import List

import numpy
import pandas as pd

from numpy.testing import assert_array_equal
from pandas.util.testing import assert_frame_equal
from pytest import fixture

from dbnd import band, output, task
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase
from targets import FileTarget
from targets.types import DataList


@task(result=output.data_list_str)
def t_f_b(t_input2):
    # type: (DataList[str]) -> List[str]
    return ["s_%s" % s for s in t_input2]


class TestInlineCalls(TargetTestBase):
    @fixture
    def target_1_2(self):
        t = self.target("file.txt")
        t.as_object.write("1\n2\n")
        return t

    def test_inline_call(self, target_1_2):
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
