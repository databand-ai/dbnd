from dbnd_spark.spark_config import SparkConfig


class TestSparkConfig(object):
    def test_spark_config(self):
        assert SparkConfig()
