import logging

from dbnd import new_dbnd_context
from dbnd_spark.spark import PySparkTask
from dbnd_spark.spark_config import SparkConfig


logger = logging.getLogger(__name__)


class SimpleSparkTask(PySparkTask):
    python_script = "test_script.py"


class TestTaskMetaBuild(object):
    def test_verbose_build(self):
        with new_dbnd_context(conf={"task_build": {"verbose": "True"}}):
            task = SimpleSparkTask(override={SparkConfig.driver_memory: "test_driver"})
            assert task.spark_config.driver_memory == "test_driver"
