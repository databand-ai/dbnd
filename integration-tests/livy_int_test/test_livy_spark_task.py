import random

import pytest

from databand import dbnd_config
from dbnd._core.errors import DatabandRunError
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_spark.spark_config import SparkConfig
from dbnd_test_scenarios.spark.spark_tasks import (
    WordCountPySparkTask,
    WordCountTask,
    WordCountThatFails,
)
from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline
from targets import target


TEXT_FILE = "/app/integration-test/vegetables_for_greek_salad.txt"

conf_override = {
    "task": {"spark_engine": "livy"},
    "livy": {"url": "http://livy:8998"},
    SparkConfig.jars: "",
    SparkConfig.main_jar: "/app/spark_jvm/target/ai.databand.examples-1.0-SNAPSHOT.jar",
}


class TestEmrSparkTasks(object):
    def test_word_count_spark(self):
        with dbnd_config({"core": {"tracker_api": "disabled"}}):
            actual = WordCountTask(
                text=TEXT_FILE,
                task_version=str(random.random()),
                override=conf_override,
            )
            actual.dbnd_run()
            print(target(actual.counters.path, "part-00000").read())

    def test_word_count_pyspark(self):
        with dbnd_config({"core": {"tracker_api": "disabled"}}):
            actual = WordCountPySparkTask(
                text=TEXT_FILE,
                task_version=str(random.random()),
                override=conf_override,
            )
            actual.dbnd_run()
            print(target(actual.counters.path, "part-00000").read())

    @pytest.mark.skip("Some issue with cloud pickler")
    def test_word_spark_with_error(self):
        with dbnd_config({"core": {"tracker_api": "disabled"}}):
            actual = WordCountThatFails(
                text=TEXT_FILE,
                task_version=str(random.random()),
                override=conf_override,
            )
            with pytest.raises(DatabandRunError):
                actual.dbnd_run()

    def test_word_count_inline(self):
        with dbnd_config({"core": {"tracker_api": "disabled"}}):
            assert_run_task(
                word_count_inline.t(
                    text=TEXT_FILE,
                    task_version=str(random.random()),
                    override=conf_override,
                )
            )
