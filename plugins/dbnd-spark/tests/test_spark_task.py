import pytest

from pytest import mark

from dbnd import parameter
from dbnd._core.errors import DatabandRunError
from dbnd.tasks import Config
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_airflow_contrib.mng_connections import set_connection
from dbnd_test_scenarios.spark.spark_tasks import (
    WordCountPySparkTask,
    WordCountTask,
    WordCountThatFails,
)
from targets import target


class LocalSparkTestConfig(Config):
    spark_home = parameter[str]


@pytest.fixture()
def spark_config(databand_test_context):
    config = LocalSparkTestConfig()

    set_connection(
        conn_id="spark_default",
        host="local",
        conn_type="docker",
        extra='{"master":"local","spark-home": "%s"}' % config.spark_home,
    )
    return config


@mark.spark
class TestSparkTasksLocally(object):
    @pytest.fixture(autouse=True)
    def spark_conn(self, spark_config):
        self.config = spark_config

    def test_word_count_pyspark(self):
        actual = WordCountPySparkTask(text=__file__)
        actual.dbnd_run()
        print(target(actual.counters.path, "part-00000").read())

    def test_word_spark(self):
        actual = WordCountTask(text=__file__)
        actual.dbnd_run()
        print(target(actual.counters.path, "part-00000").read())

    def test_word_spark_with_error(self):
        actual = WordCountThatFails(text=__file__)
        with pytest.raises(DatabandRunError):
            actual.dbnd_run()

    def test_spark_inline(self):
        from dbnd_test_scenarios.spark.spark_tasks_inline import word_count_inline

        assert_run_task(word_count_inline.t(text=__file__))

    def test_spark_io(self):
        from dbnd_test_scenarios.spark.test_spark_io import dataframes_io_pandas_spark

        assert_run_task(dataframes_io_pandas_spark.t(text=__file__))
