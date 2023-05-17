# © Copyright Databand.ai, an IBM Company 2022
from dbnd_spark.spark_config import SparkConfig


class TestSparkConfig(object):
    def test_spark_config(self):
        assert SparkConfig()

    def test_livy_config(self):
        from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline

        t = word_count_inline.t(text=__file__, spark_engine="livy")
        assert t.spark_engine.task_name == "livy"
