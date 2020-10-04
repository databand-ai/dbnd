import pandas as pd
import pytest

from dbnd.testing.test_config_setter import add_test_configuration


pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]


def pytest_configure(config):
    add_test_configuration(__file__)


@pytest.fixture
def pandas_data_frame():
    df = pd.DataFrame(
        {
            "Names": pd.Series(["Bob", "Jessica", "Mary", "John", "Mel"], dtype="str"),
            "Births": pd.Series([968, 155, 77, 578, 973], dtype="int"),
            "Weights": pd.Series([12.3, 23.4, 45.6, 56.7, 67.8], dtype="float"),
            "Married": pd.Series([True, False, True, False, True], dtype="bool"),
        }
    )
    return df


@pytest.fixture
def pandas_data_frame_histograms(pandas_data_frame):
    return {
        "Births": ([2, 0, 1, 0, 2], [77.0, 256.0, 435.0, 614.0, 793.0, 973.0]),
        "Married": ([3, 2], [True, False]),
        "Names": ([1, 1, 1, 1, 1], ["Bob", "Jessica", "John", "Mary", "Mel"]),
        "Weights": (
            [1, 1, 0, 1, 2],
            [12.3, 23.4, 34.5, 45.599999999999994, 56.7, 67.8],
        ),
    }


@pytest.fixture
def pandas_data_frame_stats(pandas_data_frame):
    return {
        "Births": {
            "25%": 155.0,
            "50%": 578.0,
            "75%": 968.0,
            "count": 5.0,
            "distinct": 5,
            "max": 973.0,
            "mean": 550.2,
            "min": 77.0,
            "non-null": 5.0,
            "null-count": 0,
            "std": 428.42467249214303,
            "stddev": 428.42467249214303,
            "type": "long",
        },
        "Married": {
            "count": 5,
            "distinct": 2,
            "non-null": 5,
            "null-count": 0,
            "type": "boolean",
        },
        "Names": {
            "count": 5,
            "distinct": 5,
            "non-null": 5,
            "null-count": 0,
            "type": "string",
        },
        "Weights": {
            "25%": 23.4,
            "50%": 45.6,
            "75%": 56.7,
            "count": 5.0,
            "distinct": 5,
            "max": 67.8,
            "mean": 41.160000000000004,
            "min": 12.3,
            "non-null": 5.0,
            "null-count": 0,
            "std": 23.01744990219377,
            "stddev": 23.01744990219377,
            "type": "double",
        },
    }


@pytest.fixture
def spark_data_frame(pandas_data_frame, spark_session):
    df = spark_session.createDataFrame(pandas_data_frame)
    return df


@pytest.fixture
def spark_data_frame_histograms(pandas_data_frame_histograms):
    return pandas_data_frame_histograms


@pytest.fixture
def spark_data_frame_stats(pandas_data_frame_stats):
    return pandas_data_frame_stats
