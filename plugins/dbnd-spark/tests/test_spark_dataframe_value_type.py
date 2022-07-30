# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd._core.tracking.schemas.column_stats import ColumnStatsArgs
from dbnd_spark.spark_targets import SparkDataFrameValueType
from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values.pandas_values import DataFrameValueType


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
            column_type="StringType",
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
            column_type="LongType",
            records_count=5,
            distinct_count=None,
            null_count=0,
            unique_count=None,
            most_freq_value=None,
            most_freq_value_count=None,
            mean_value="550.2",
            min_value="77",
            max_value="973",
            std_value="428.42467249214303",
            quartile_1="155",
            quartile_2="578",
            quartile_3="968",
        ),
        ColumnStatsArgs(
            column_name="Weights",
            column_type="DoubleType",
            records_count=5,
            distinct_count=None,
            null_count=0,
            unique_count=None,
            most_freq_value=None,
            most_freq_value_count=None,
            mean_value="41.160000000000004",
            min_value="12.3",
            max_value="67.8",
            std_value="23.01744990219377",
            quartile_1="23.4",
            quartile_2="45.6",
            quartile_3="56.7",
        ),
    ]
