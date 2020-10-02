import logging

import pytest
import six

from dbnd.testing.helpers_pytest import assert_run_task


if six.PY2:
    pytestmark = pytest.mark.skip()  # py2 styling
else:
    from dbnd_examples.tutorial_syntax import T22_function_with_different_inputs
    from dbnd_examples.tutorial_syntax.T11_tasks_pipeline import (
        pipe_operations,
        pipeline_into_pipeline,
    )
    from dbnd_examples.tutorial_syntax.T24_function_with_pandas_numpy import (
        f_test_pandas_numpy_flow,
    )
logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
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

    def test_pipe_operations_pipe(self):
        task = assert_run_task(pipe_operations.task("my_pipe"))
        assert "operation_z(my_pipe) -> operation_x -> operation_y" == task.result.load(
            str
        )

    def test_pipe_operations_pipelines(self):
        task = assert_run_task(pipeline_into_pipeline.task())

        actual = task.result.load(str)
        print(actual)
        assert (
            "operation_z(operation_z(pipe) -> operation_x -> operation_y)"
            " -> operation_x -> operation_y" == actual
        )

    def test_pandas_numpy(self):
        assert_run_task(f_test_pandas_numpy_flow.task())
