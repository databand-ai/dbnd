# © Copyright Databand.ai, an IBM Company 2022

import logging
import random

import pytest

from dbnd import config
from dbnd._core.errors import DatabandRunError
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_spark.spark_config import SparkConfig
from dbnd_test_scenarios.spark.spark_tasks import (
    WordCountPySparkTask,
    WordCountTask,
    WordCountThatFails,
)
from targets import target


conf_override = {
    "task": {"spark_engine": "livy"},
    "livy": {"url": "http://localhost:8998"},
    SparkConfig.jars: "",
    SparkConfig.main_jar: "",
}


@pytest.mark.livy
class TestLivySparkTasks(object):
    # add back java code test
    @pytest.mark.skip
    def test_word_count_spark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountTask(
            text=config.get("livy_tests", "text"),
            task_version=str(random.random()),
            override=conf_override,
        )
        actual.dbnd_run()
        print(target(actual.counters.path, "part-00000").read())

    @pytest.mark.skip
    def test_word_count_pyspark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountPySparkTask(
            text=config.get("livy_tests", "text"),
            task_version=str(random.random()),
            override=conf_override,
        )
        actual.dbnd_run()
        print(target(actual.counters.path, "part-00000").read())

    @pytest.mark.skip
    def test_word_spark_with_error(self):
        actual = WordCountThatFails(
            text=config.get("livy_tests", "text"),
            task_version=str(random.random()),
            override=conf_override,
        )
        with pytest.raises(DatabandRunError):
            actual.dbnd_run()

    @pytest.mark.skip
    def test_word_count_inline(self):
        from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline

        assert_run_task(
            word_count_inline.t(
                text=config.get("livy_tests", "text"),
                task_version=str(random.random()),
                override=conf_override,
            )
        )
