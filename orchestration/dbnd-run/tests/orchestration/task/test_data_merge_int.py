# © Copyright Databand.ai, an IBM Company 2022

from typing import List

from dbnd import band, output, task
from dbnd.utils import data_combine
from dbnd_run.testing.helpers import assert_run_task


@task
def calc_1(value=1.0):
    # type: (float)-> List[int]
    return [1, 2]


@task
def calc_2(value=1.0):
    # type: (float)-> List[int]
    return [3, 4]


@task(result=output.json)
def sum_values(values):
    # type: (List)-> int
    return sum(values)


@band
def pipe1(value=1.0):
    # type: (float)-> int
    c1 = calc_1(value)
    c2 = calc_2(value)
    data = data_combine([c1, c2])

    return sum_values(data)


class TestTaskMergeInt(object):
    def test_wire_int_value(self):
        target = pipe1.t(0.5)
        assert_run_task(target)
        assert target.result.load(int) == 10
