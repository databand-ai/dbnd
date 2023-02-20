# © Copyright Databand.ai, an IBM Company 2022

from dbnd_spark.spark_config import SparkConfig


class TestSparkConfig(object):
    def test_spark_config(self):
        assert SparkConfig()
