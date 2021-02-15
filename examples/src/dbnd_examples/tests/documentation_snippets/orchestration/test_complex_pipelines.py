import logging

import pytest
import six

from dbnd.testing.helpers_pytest import assert_run_task


if six.PY2:
    pytestmark = pytest.mark.skip()  # py2 styling
else:
    from dbnd_examples.tutorial_syntax.T11_tasks_pipeline import (
        pipe_operations,
        pipeline_into_pipeline,
    )

logger = logging.getLogger(__name__)


class TestTutorialSyntax(object):
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
