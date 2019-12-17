import logging
import random

import pytest

from pytest import fixture, mark

from dbnd import config
from dbnd._core.constants import CloudType, SparkClusters
from dbnd._core.errors import DatabandExecutorError
from dbnd._core.inline import run_task
from dbnd._core.settings import EnvConfig
from dbnd.testing import assert_run_task
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
        run_task(actual)
        print(target(actual.counters.path, "part-00000").read())

    def test_word_count_pyspark(self):
        logging.info("Running %s", WordCountPySparkTask)
        actual = WordCountPySparkTask(
            text=TEXT_FILE, task_version=str(random.random()), override=conf_override
        )
        run_task(actual)
        print(target(actual.counters.path, "part-00000").read())

    def test_word_spark_with_error(self):
        actual = WordCountThatFails(
            text=TEXT_FILE, task_version=str(random.random()), override=conf_override
        )
        with pytest.raises(DatabandExecutorError):
            run_task(actual)

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
