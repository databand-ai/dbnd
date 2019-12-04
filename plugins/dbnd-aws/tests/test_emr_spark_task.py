import logging
import random

import pytest

from pytest import fixture, mark

from dbnd._core.errors import DatabandExecutorError
from dbnd._core.inline import run_task
from dbnd.testing import assert_run_task
from dbnd_test_scenarios.spark.spark_tasks import (
    WordCountPySparkTask,
    WordCountTask,
    WordCountThatFails,
)
from targets import target


@fixture
def databand_context_kwargs():
    return dict(
        conf={
            "task": {"task_env": "aws"},
            "aws": {"spark": "emr"},
            "emr": {"cluster": "dev-shafran-2"},
        }
    )


# TODO: add back this test
@mark.aws
@pytest.mark.skip
class TestEmrSparkTasks(object):
    def test_word_count_spark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountTask(
            text="s3://databand-example/data/some_example.txt",
            task_version=str(random.random()),
        )
        run_task(actual)
        print(target(actual.counters.path, "part-00000").read())

    def test_word_count_pyspark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountPySparkTask(
            text="s3://databand-example/data/some_example.txt",
            task_version=str(random.random()),
        )
        run_task(actual)
        print(target(actual.counters.path, "part-00000").read())

    def test_word_spark_with_error(self):
        actual = WordCountThatFails(
            text="s3://databand-example/data/some_example.txt",
            task_version=str(random.random()),
        )
        with pytest.raises(DatabandExecutorError):
            run_task(actual)

    def test_word_count_inline(self):
        from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline

        assert_run_task(
            word_count_inline.t(
                text="s3://databand-example/data/some_example.txt",
                task_version=str(random.random()),
            )
        )
