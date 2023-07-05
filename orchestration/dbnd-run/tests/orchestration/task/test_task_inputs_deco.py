# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List

from pandas import DataFrame
from pandas.util.testing import assert_frame_equal

from dbnd import output, task
from dbnd.testing.orchestration_utils import TargetTestBase
from dbnd_run.testing.helpers import assert_run_task
from targets import target
from targets.types import DataList, Path, PathStr


logger = logging.getLogger(__name__)

_value_1_2 = ["1", "2"]


@task
def t_f_data_list_str(a):
    # type: (DataList[str]) -> List[str]
    assert a == _value_1_2
    return a[:1]


# invocation with args (without names) works
@task
def t_f_data_list_str_with_int_default(a, b=4):
    # type: (DataList[str], int) -> int
    assert b == 2
    assert _value_1_2 == a
    return b


@task(b=output[Path])
def t_f_pathlib(a, b):
    # type: (Path, Path) -> str
    assert isinstance(a, Path), type(a)
    assert isinstance(b, Path), type(b)

    target(str(b)).mkdir_parent()
    with open(str(b), "w") as fp:
        fp.write("ok")
    return str(a)


class TestDecoratorTaskInputs(TargetTestBase):
    def test_input_lines_direct_call(self, target_1_2):
        actual = t_f_data_list_str(a=_value_1_2)
        assert actual == _value_1_2[:1]

    def test_input_lines_task(self, target_1_2):
        assert_run_task(t_f_data_list_str.t(a=target_1_2))

    def test_input_lines_task_with_path(self, target_1_2):
        assert_run_task(t_f_data_list_str.t(a=target_1_2.path))

    def test_input_lines_as_filename_args(self, target_1_2):
        t_f_data_list_str_with_int_default(target_1_2.readlines(), 2)

        target = t_f_data_list_str_with_int_default.t(target_1_2.path, 2)
        assert_run_task(target)

    def test_input_filename(self, target_1_2):
        @task
        def t_f_path(a):
            # type: (PathStr) -> str
            assert target_1_2.path == a
            return a

        t_f_path(a=target_1_2.path)
        assert_run_task(t_f_path.t(a=target_1_2))

    def test_input_pathlib_as_target(self, target_1_2):
        assert_run_task(t_f_pathlib.t(a=target_1_2))

    def test_input_pathlib_as_pathlib(self, target_1_2):
        assert_run_task(t_f_pathlib.t(a=Path(target_1_2)))

    def test_input_df_func_call(self, pandas_data_frame):
        @task
        def t_f_df(a):
            # type: (DataFrame) -> int
            assert_frame_equal(pandas_data_frame, a)
            return 1

        t_f_df(a=pandas_data_frame)

    def test_input_df_simple(self, pandas_data_frame):
        my_target = self.target("file.parquet")
        my_target.write_df(pandas_data_frame)

        @task
        def t_f_df_simple(a):
            # type: (DataFrame) -> int
            assert_frame_equal(pandas_data_frame, a)
            return 1

        assert_run_task(t_f_df_simple.t(a=my_target))

    def test_input_df_inline_value(self, pandas_data_frame):
        my_target = self.target("file.parquet")
        my_target.write_df(pandas_data_frame)

        @task
        def t_f_df_inline_value(a):
            # type: (DataFrame) -> int
            assert_frame_equal(pandas_data_frame, a)
            return 1

        assert_run_task(t_f_df_inline_value.t(a=pandas_data_frame))
