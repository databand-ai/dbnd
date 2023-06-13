# © Copyright Databand.ai, an IBM Company 2022

import logging

import pytest

from dbnd import relative_path
from dbnd._core.errors import DatabandRunError
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_spark.spark_config import SparkConfig
from dbnd_test_scenarios.spark.spark_tasks import (
    WordCountPySparkTask,
    WordCountTask,
    WordCountThatFails,
)
from targets import target


@pytest.fixture
def dbnd_config_for_test_run__user():
    return {
        "task": {"spark_engine": "livy"},
        "livy": {"url": "http://localhost:8998"},
        SparkConfig.jars: "",
        SparkConfig.main_jar: "",
    }


TEXT_INPUT = relative_path(__file__, "people.txt")


@pytest.mark.livy
@pytest.mark.skip
class TestLivySparkTasks(object):
    # add back java code test
    def test_word_count_spark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountTask(text=TEXT_INPUT)
        actual.dbnd_run()
        print(target(actual.counters.path, "part-00000").read())

    def test_word_count_pyspark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountPySparkTask(text=TEXT_INPUT)
        actual.dbnd_run()
        print(target(actual.counters.path, "part-00000").read())

    def test_word_spark_with_error(self):
        actual = WordCountThatFails(text=TEXT_INPUT)
        with pytest.raises(DatabandRunError):
            actual.dbnd_run()

    def test_word_count_inline(self):
        from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline

        assert_run_task(word_count_inline.t(text=TEXT_INPUT))
