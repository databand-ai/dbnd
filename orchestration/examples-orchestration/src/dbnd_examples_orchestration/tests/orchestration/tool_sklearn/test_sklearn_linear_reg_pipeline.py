# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_examples_orchestration.orchestration.tool_sklearn.sklearn_example import (
    linear_reg_pipeline,
)

from dbnd_run.testing.helpers import assert_run_task


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def test_sklearn_tracking():
    task = assert_run_task(linear_reg_pipeline.task())
    assert task is not None
    assert task.result is not None
