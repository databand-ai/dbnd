# © Copyright Databand.ai, an IBM Company 2022

import logging
import random

import pytest

from dbnd import config
from dbnd._core.constants import CloudType, SparkClusters
from dbnd._core.errors import DatabandRunError
from dbnd._core.settings import EnvConfig
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_aws.emr.emr_config import EmrConfig
from dbnd_spark.spark_config import SparkConfig
from dbnd_test_scenarios.spark.spark_tasks import (
    WordCountPySparkTask,
    WordCountTask,
    WordCountThatFails,
)
from targets import target


conf_override = {
    "task": {"task_env": CloudType.aws},
    EnvConfig.spark_config: SparkClusters.emr,
    EmrConfig.cluster: config.get("aws_tests", "cluster"),
    SparkConfig.jars: "",
    SparkConfig.main_jar: "",
}

TEXT_FILE = config.get("aws_tests", "text_file")


@pytest.mark.emr
class TestEmrSparkTasks(object):
    # add back java code test
    @pytest.mark.skip
    def test_word_count_spark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountTask(
            text=TEXT_FILE, task_version=str(random.random()), override=conf_override
        )
        actual.dbnd_run()
        print(target(actual.counters.path, "part-00000").read())

    def test_word_count_pyspark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountPySparkTask(
            text=TEXT_FILE, task_version=str(random.random()), override=conf_override
        )
        actual.dbnd_run()
        print(target(actual.counters.path, "part-00000").read())

    def test_word_spark_with_error(self):
        actual = WordCountThatFails(
            text=TEXT_FILE, task_version=str(random.random()), override=conf_override
        )
        with pytest.raises(DatabandRunError):
            actual.dbnd_run()

    # @pytest.mark.skip
    def test_word_count_inline(self):
        from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline

        assert_run_task(
            word_count_inline.t(
                text=TEXT_FILE,
                task_version=str(random.random()),
                override=conf_override,
            )
        )

    @pytest.mark.skip
    def test_io(self):
        from dbnd_test_scenarios.spark.spark_io_inline import dataframes_io_pandas_spark

        assert_run_task(
            dataframes_io_pandas_spark.t(
                text=TEXT_FILE,
                task_version=str(random.random()),
                override=conf_override,
            )
        )
