import pandas as pd
import pytest

from dbnd import dbnd_config, relative_path


pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
]

dbnd_config.set_from_config_file(relative_path(__file__, "databand-test.cfg"))


@pytest.fixture
def pandas_data_frame():
    names = ["Bob", "Jessica", "Mary", "John", "Mel"]
    births = [968, 155, 77, 578, 973]
    df = pd.DataFrame(data=list(zip(names, births)), columns=["Names", "Births"])
    return df


@pytest.fixture
def pandas_data_frame_histograms(pandas_data_frame):
    return {
        "Names": ([1, 1, 1, 1, 1], ["Mary", "Bob", "John", "Jessica", "Mel"]),
        "Births": ([2, 0, 1, 0, 2], [77.0, 256.0, 435.0, 614.0, 793.0, 973.0],),
    }


@pytest.fixture
def pandas_data_frame_stats(pandas_data_frame):
    return {
        "Names": {"distinct": 5, "null-count": 0, "non-null": 5, "count": 5},
        "Births": {
            "count": 5,
            "mean": 550.2,
            "stddev": 428.42467249214303,
            "min": 77.0,
            "25%": 155.0,
            "50%": 578.0,
            "75%": 968.0,
            "max": 973.0,
            "std": 428.42467249214303,
            "distinct": 5,
            "null-count": 0,
            "non-null": 5,
        },
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
