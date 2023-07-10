# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging

from pathlib import Path

from dbnd_examples_orchestration.orchestration.tutorial_syntax.T02_task_parameters import (
    f_path_types,
    f_task_parameters,
)

from dbnd import pipeline
from targets import target


logger = logging.getLogger(__name__)

EXPECTED_STR = (
    "'1' 'False' 'strstr'",
    "'2018-01-01 00:00:00' '2018-01-01' '1 day, 0:00:00'",
)

some_existing_file = __file__


@pipeline
def f_run_all_simple():
    result = f_task_parameters(
        1,
        False,
        "strstr",
        v_datetime=datetime.datetime(year=2018, month=1, day=1),
        v_date=datetime.date(year=2018, month=1, day=1),
        v_period=datetime.timedelta(days=1),
        v_list=[1, "1"],
        v_list_obj=[2, "2"],
        v_list_str=["str", "str"],
        v_list_int=[1, 2],
        v_list_date=[datetime.date(year=2018, month=1, day=1)],
    )
    paths = f_path_types(
        pathlib_path=Path(some_existing_file),
        str_as_path=some_existing_file,
        target_path=target(some_existing_file),
    )

    return result, paths


class TestTaskParameters(object):
    def test_run_all_as_regular_function(self):
        result, paths = f_run_all_simple()
        # this was a normal execution of the function, no databand required!

        assert result

    def test_run_all_task(self):
        # the test code runs outsider @band context, we need to explicitly state .task() for a function
        # otherwise it will be just called like a regular function

        run_result = f_run_all_simple.task().dbnd_run()

        actual = run_result.root_task.result[0].load(object)
        assert actual
