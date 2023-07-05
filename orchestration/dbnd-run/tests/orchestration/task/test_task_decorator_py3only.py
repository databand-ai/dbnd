# © Copyright Databand.ai, an IBM Company 2022

import datetime
import json
import logging

from typing import List, Optional, Union

import pandas as pd
import six

from pandas import DataFrame
from pandas.util.testing import assert_frame_equal

from dbnd import dbnd_run_cmd, task
from dbnd.testing.orchestration_utils import TargetTestBase
from dbnd_run.testing.helpers import assert_run_task
from targets.types import LazyLoad


logger = logging.getLogger(__name__)


@task
def t_d_1(
    a_str: str,
    b_datetime: datetime.datetime,
    c_timedelta: datetime.timedelta,
    d_int: int,
) -> DataFrame:
    assert a_str == "1"
    assert b_datetime.isoformat() == "2018-01-01T10:10:10.100000+00:00"
    assert c_timedelta == datetime.timedelta(days=5)
    assert d_int == 1
    return pd.DataFrame(data=[[1, 1], [2, 2]], columns=["c1", "c2"])


@task
def t_d_df(some_df: DataFrame) -> DataFrame:
    assert isinstance(some_df, DataFrame)
    return some_df.tail(1)


class TestTaskDecoratorsPY3(TargetTestBase):
    def test_simple_types(self):
        target_cls = t_d_1.task
        assert target_cls.a_str.value_type.type == str
        assert target_cls.b_datetime.value_type.type == datetime.datetime
        assert target_cls.c_timedelta.value_type.type == datetime.timedelta
        assert target_cls.d_int.value_type.type == int
        assert target_cls.result.value_type.type == DataFrame

    def test_simple_func(self):
        assert_run_task(
            t_d_1.task(
                a_str="1", b_datetime="2018-01-01T101010.1", c_timedelta="5d", d_int="1"
            )
        )

    def test_simple_cli(self):
        dbnd_run_cmd(
            [
                "t_d_1",
                "-r",
                "a_str=1",
                "-r",
                "b_datetime=2018-01-01T101010.1",
                "-r",
                " c_timedelta=5d",
                "--set",
                json.dumps({"t_d_1": {"d_int": 1}}),
            ]
        )

    def test_df_cli(self, pandas_data_frame_on_disk):
        my_target = pandas_data_frame_on_disk[1]
        dbnd_run_cmd(["t_d_df", "-r", "some_df=" + my_target.path])

    def test_df_func(self, pandas_data_frame_on_disk):
        df, df_target = pandas_data_frame_on_disk

        res = assert_run_task(t_d_df.task(df_target.path))
        assert_frame_equal(
            df.tail(1).reset_index(drop=True),
            res.result.read_df().reset_index(drop=True),
        )

    def test_optional_types(self):
        @task
        def t_f(p_str: Optional[str], p_int: Optional[int]) -> Optional[str]:
            return "ok"

        assert t_f.task.p_str.value_type.type == str
        assert t_f.task.p_int.value_type.type == int
        assert t_f.task.result.value_type.type == str

    def test_union_types(self):
        @task
        def t_f(p_str: Union[str, int], p_int: Union[int, None]) -> Union[str, None]:
            return "ok"

        assert t_f.task.p_str.value_type.type == str
        assert t_f.task.p_int.value_type.type == int
        assert t_f.task.result.value_type.type == str

    def test_lazy_types(self):
        @task
        def t_f(
            p_str: Union[str, LazyLoad],
            p_int: Union[int, LazyLoad],
            p_list: Union[List[int], LazyLoad],
        ) -> Union[str, None]:
            assert isinstance(p_str, six.string_types)
            assert isinstance(p_int, int)
            assert isinstance(p_list, List)
            return "ok"

        p_str = self.target("p_str.txt")
        p_str.dump("testtest")
        p_int = self.target("p_int.txt")
        p_int.dump(2)

        p_list = self.target("p_list.txt")
        p_list.dump([1, 2], value_type=List[int])
