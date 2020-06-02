import pandas as pd
import pytest


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
def pandas_data_frame_histograms(pandas_data_frame):
    return {
        "Births": (
            [2, 0, 1, 0, 2],
            [77.0, 256.2, 435.4, 614.5999999999999, 793.8, 973.0],
        ),
    }


@pytest.fixture
def pandas_data_frame_stats(pandas_data_frame):
    return {
        "Births": {
            "count": 5.0,
            "mean": 550.2,
            "std": 428.4246724921,
            "min": 77.0,
            "25%": 155.0,
            "50%": 578.0,
            "75%": 968.0,
            "max": 973.0,
        }
    }


@pytest.fixture
def spark_data_frame(pandas_data_frame):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("PytestFixture").getOrCreate()
    df = spark.createDataFrame(pandas_data_frame)
    return df


@pytest.fixture
def spark_data_frame_histograms(pandas_data_frame_histograms):
    return pandas_data_frame_histograms


@pytest.fixture
def spark_data_frame_stats(pandas_data_frame_stats):
    return pandas_data_frame_stats
