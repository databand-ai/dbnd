# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Tuple

import pandas as pd

from pandas import DataFrame

from dbnd import task
from dbnd.testing.orchestration_utils import TargetTestBase
from dbnd_run.testing.helpers import assert_run_task


logger = logging.getLogger(__name__)


@task(result=("features", "scores"))
def t_d_multiple_return(p: int) -> (DataFrame, int):
    return pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]), 5


@task(result=("features", "scores"))
def t_d_multiple_tuple_return(p: int) -> Tuple[DataFrame, int]:
    return pd.DataFrame(data=[[p, 1]], columns=["c1", "c2"]), 5


class TestTaskDecoratorOutputPY3(TargetTestBase):
    def test_simple_func(self):
        task = assert_run_task(t_d_multiple_return.task(p=3))
        assert str(task.features.config) == ".csv"
        assert str(task.scores.config) == ".pickle"

    def test_tuple_return(self):
        task = assert_run_task(t_d_multiple_tuple_return.task(p=3))
        assert str(task.features.config) == ".csv"
        assert str(task.scores.config) == ".pickle"
