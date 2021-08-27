import datetime
import logging

import pytest
import six

from dbnd import PythonTask, parameter
from dbnd.testing.helpers_pytest import assert_run_task


if six.PY2:
    pytestmark = pytest.mark.skip()  # py2 styling
else:
    from dbnd_examples.orchestration.tutorial_syntax import (
        T22_function_with_different_inputs,
    )

logger = logging.getLogger(__name__)


class DateTask(PythonTask):
    date = parameter[datetime.date]


class DateTask2(DateTask):
    other = parameter(significant=False)[str]


class TestTaskParameters(object):
    def test_run_all_as_regular_function(self):
        actual = T22_function_with_different_inputs.f_test_flow()
        # this was a normal execution of the function, no databand required!

        assert actual == "OK"

    def test_run_all_task(self):
        task = T22_function_with_different_inputs.f_test_flow.task()
        # this code runs outsider @band context, we need to explicitly state .task(),
        #  otherwise function will be executed in place
        # so 'task' is a Task object, it's a definition of the Pipeline, it still not executed

        assert_run_task(task)
        actual = task.result.load(str)
        assert actual == "OK"

    def test_run_with_strings(self):
        # inside function we have assert, so the strings are actually going to be real datetime object
        task = T22_function_with_different_inputs.f_datetime_types.dbnd_run(
            "2018-01-01T000000.0000", "2018-01-01", "1d"
        ).task
        actual = task.result.load(str)
        logger.info("result: %s", actual)
        # we have string like : '2018-01-01 00:00:00+00:00' in the result
        # it changes from machine to machine :(
        assert "'2018-01-01' '1 day, 0:00:00'" in actual

    def test_task_parameters(self):
        a = datetime.date(2014, 1, 21)
        b = datetime.date(2014, 1, 21)

        c = DateTask2(date=a, other="foo")
        d = DateTask2(date=b, other="bar")

        assert c.other == "foo"
        assert d.other == "foo"
        assert c is d
