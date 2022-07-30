# © Copyright Databand.ai, an IBM Company 2022

from typing import Any

import pandas as pd

from dbnd import band, output, task
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd.utils import data_combine


@task
def calc_1(value=1.0):
    # type: (float)-> pd.DataFrame
    return pd.DataFrame(data=[[1, 1], [2, 2]], columns=["c1", "c2"])


@task
def calc_2(value=1.0):
    # type: (float)-> pd.DataFrame
    return pd.DataFrame(data=[[3, 3], [4, 4]], columns=["c1", "c2"])


@task(result=output.pickle)
def sum_values(values):
    # type: (pd.DataFrame)-> int
    return values.sum()


@band
def pipe1(value=1.0):
    # type: (float)-> Any
    c1 = calc_1(value)
    c2 = calc_2(value)
    data = data_combine([c1, c2])

    return sum_values(data)


class TestTaskMergeDataFrame(object):
    def test_wire_value(self):
        target = pipe1.t(0.5)
        assert_run_task(target)

        expected = pd.Series([10, 10], index=["c1", "c2"])
        assert pd.Series.equals(target.result.load(object), expected)
