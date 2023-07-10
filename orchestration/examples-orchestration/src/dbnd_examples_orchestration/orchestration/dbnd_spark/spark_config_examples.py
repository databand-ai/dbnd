# © Copyright Databand.ai, an IBM Company 2022

import logging

import pyspark.sql as spark
import six

from databand import output, parameter
from dbnd import config, dbnd_context
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd_spark import SparkConfig
from dbnd_spark.spark import PySparkInlineTask, spark_task
from targets.target_config import FileFormat


logger = logging.getLogger(__name__)


@spark_task(result=output.save_options(FileFormat.csv, header=True)[spark.DataFrame])
def custom_load_save_options(
    data=parameter.load_options(FileFormat.csv, header=False, sep="\t")[spark.DataFrame]
):
    print(data.show())
    return data


class AdvancedSparkConfigExample(PySparkInlineTask):
    text = parameter.csv[spark.DataFrame]
    counters = output.txt.data
    my_p = parameter.value(3)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Every parameter of the task can be changed including spark config.
        # However, you can not use "configuration" (SparkConfig.driver_memory)
        # for that, as all config objects are calculated and created already.

        # we can change some spark config param based on other param
        if self.env == "local":
            self.spark_config.driver_memory = "2G"
        else:
            self.spark_config.driver_memory = "4G"

        self.my_p = 10

    def run(self):
        """simple map function, it will run inside spark context (after submission by dbnd)"""
        from operator import add

        lines = self.text.rdd.map(lambda r: r[0])
        counts = (
            lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
        )
        counts.saveAsTextFile(str(self.counters))


#######################
## very advanced usage
## add extra config to spark tasks based on task class and env


class AdvancedConfigTaskMetaclass(TaskMetaclass):
    """
    The Metaclass of :py:class:`Task`.
    """

    def __call__(cls, *args, **kwargs):
        """
        Extension of Task Metaclass, we want to add our own configuration based on some values
        so the moment task is created, that config is used.
        """

        config_values = cls.get_custom_config()

        # create new config layer, so when we are out of this process -> config is back to the previous value
        with config(
            config_values=config_values,
            source=cls.task_definition.task_passport.format_source_name(
                ".get_custom_config"
            ),
        ):
            return super(AdvancedConfigTaskMetaclass, cls).__call__(*args, **kwargs)


@six.add_metaclass(AdvancedConfigTaskMetaclass)
class PySparkInlineWithAdvancedConfig(PySparkInlineTask):
    """you can apply AdvancedConfigTaskMetaclass to specific task or to some `base` class"""

    @classmethod
    def get_custom_config(cls):
        current_env = dbnd_context().env
        logger.info("Creating task with spark config for %s", current_env)
        if current_env.name == "local":
            return {SparkConfig.num_executors: 3}
        return {SparkConfig.num_executors: 5}


class AdvancedSparkConfigAtMetaclassExample(PySparkInlineWithAdvancedConfig):
    text = parameter.csv[spark.DataFrame]
    counters = output.txt.data
    my_p = parameter.value(3)

    def run(self):
        """simple map function, it will run inside spark context (after submission by dbnd)"""
        from operator import add

        lines = self.text.rdd.map(lambda r: r[0])
        counts = (
            lines.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(add)
        )
        counts.saveAsTextFile(str(self.counters))
