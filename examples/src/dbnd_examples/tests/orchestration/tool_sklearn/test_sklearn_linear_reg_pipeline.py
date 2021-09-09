import logging

from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_examples.orchestration.tool_sklearn.sklearn_example import linear_reg_pipeline


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def test_sklearn_tracking():
    task = assert_run_task(linear_reg_pipeline.task())
    assert task is not None
    assert task.result is not None
