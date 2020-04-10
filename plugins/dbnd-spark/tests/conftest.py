import pandas as pd
import pytest

from dbnd._core.plugin.dbnd_plugins import disable_airflow_plugin


#
# disable_airflow_plugin()
pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
]


@pytest.fixture
def pandas_data_frame():
    names = ["Bob", "Jessica", "Mary", "John", "Mel"]
    births = [968, 155, 77, 578, 973]
    df = pd.DataFrame(data=list(zip(names, births)), columns=["Names", "Births"])
    return df


@pytest.fixture
def spark_data_frame(pandas_data_frame):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("PytestFixture").getOrCreate()
    df = spark.createDataFrame(pandas_data_frame)
    return df
