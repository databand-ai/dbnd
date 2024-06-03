# © Copyright Databand.ai, an IBM Company 2022
import logging

import pandas as pd
import pytest

from dbnd._core.tracking.schemas.column_stats import ColumnStatsArgs
from targets.providers.pandas.pandas_values import DataFrameValueType
from targets.providers.spark.spark_values import SparkDataFrameValueType
from targets.value_meta import ValueMeta, ValueMetaConf


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


class TestSparkDataFrameValueType(object):
    @pytest.mark.spark
    def test_spark_df_value_meta(self, spark_data_frame, spark_data_frame_stats):
        expected_data_schema = {
            "type": SparkDataFrameValueType.type_str,
            "columns": list(spark_data_frame.schema.names),
            "size.bytes": int(spark_data_frame.count() * len(spark_data_frame.columns)),
            "shape": (spark_data_frame.count(), len(spark_data_frame.columns)),
            "dtypes": {f.name: str(f.dataType) for f in spark_data_frame.schema.fields},
        }

        expected_hist_sys_metrics = {
            "boolean_histograms_and_stats_calc_time",
            "histograms_and_stats_calc_time",
            "numeric_histograms_and_stats_calc_time",
            "string_histograms_and_stats_calc_time",
        }

        meta_conf = ValueMetaConf.enabled()

        expected_value_meta = ValueMeta(
            value_preview=SparkDataFrameValueType().to_preview(
                spark_data_frame, meta_conf.get_preview_size()
            ),
            data_dimensions=(spark_data_frame.count(), len(spark_data_frame.columns)),
            data_hash=SparkDataFrameValueType().to_signature(spark_data_frame),
            data_schema=expected_data_schema,
            columns_stats=spark_data_frame_stats,
            histograms={},
        )

        df_value_meta = SparkDataFrameValueType().get_value_meta(
            spark_data_frame, meta_conf
        )

        assert df_value_meta.value_preview == expected_value_meta.value_preview
        assert df_value_meta.data_hash == expected_value_meta.data_hash
        assert df_value_meta.data_dimensions == expected_value_meta.data_dimensions
        assert df_value_meta.data_schema == expected_value_meta.data_schema

        logging.warning("actual %s", df_value_meta.columns_stats)
        logging.warning("actual %s", expected_value_meta.columns_stats)
        assert df_value_meta.columns_stats == expected_value_meta.columns_stats
        # it changes all the time, it has different formats, and it's already tested in histogram tests

        # histogram_system_metrics values are too dynamic, so checking only keys, but not values
        assert df_value_meta.histogram_system_metrics == {}

        # assert df_value_meta.histograms == expected_value_meta.histograms
        # assert attr.asdict(df_value_meta) == attr.asdict(expected_value_meta)

        pandas_data_frame = spark_data_frame.toPandas()
        pandas_value_meta = DataFrameValueType().get_value_meta(
            pandas_data_frame, meta_conf
        )
        # assert df_value_meta == pandas_value_meta


@pytest.fixture
def spark_data_frame_stats():
    return [
        ColumnStatsArgs(
            column_name="Names",
            column_type="StringType()",
            records_count=5,
            distinct_count=None,
            null_count=0,
            unique_count=None,
            most_freq_value=None,
            most_freq_value_count=None,
            mean_value=None,
            min_value=None,
            max_value=None,
            std_value=None,
            quartile_1=None,
            quartile_2=None,
            quartile_3=None,
        ),
        ColumnStatsArgs(
            column_name="Births",
            column_type="LongType()",
            records_count=5,
            distinct_count=None,
            null_count=0,
            unique_count=None,
            most_freq_value=None,
            most_freq_value_count=None,
            mean_value=550.2,
            min_value=77,
            max_value=973,
            std_value=428.42467249214303,
            quartile_1=155,
            quartile_2=578,
            quartile_3=968,
        ),
        ColumnStatsArgs(
            column_name="Weights",
            column_type="DoubleType()",
            records_count=5,
            distinct_count=None,
            null_count=0,
            unique_count=None,
            most_freq_value=None,
            most_freq_value_count=None,
            mean_value=41.160000000000004,
            min_value=12.3,
            max_value=67.8,
            std_value=23.01744990219377,
            quartile_1=23.4,
            quartile_2=45.6,
            quartile_3=56.7,
        ),
    ]
