import pytest

from dbnd_spark.spark_targets import SparkDataFrameValueType
from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values.pandas_values import DataFrameValueType


class TestSparkDataFrameValueType(object):
    @pytest.mark.spark
    def test_spark_df_value_meta(
        self, spark_data_frame, spark_data_frame_histograms, spark_data_frame_stats
    ):
        expected_data_schema = {
            "type": SparkDataFrameValueType.type_str,
            "columns": list(spark_data_frame.schema.names),
            "size": int(spark_data_frame.count() * len(spark_data_frame.columns)),
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
            descriptive_stats=spark_data_frame_stats,
            histograms=spark_data_frame_histograms,
        )

        df_value_meta = SparkDataFrameValueType().get_value_meta(
            spark_data_frame, meta_conf
        )

        assert df_value_meta.value_preview == expected_value_meta.value_preview
        assert df_value_meta.data_hash == expected_value_meta.data_hash
        assert df_value_meta.data_dimensions == expected_value_meta.data_dimensions
        assert df_value_meta.data_schema == expected_value_meta.data_schema
        # it changes all the time, it has different formats, and it's already tested in histogram tests
        # assert df_value_meta.descriptive_stats == expected_value_meta.descriptive_stats

        # histogram_system_metrics values are too dynamic, so checking only keys, but not values
        assert (
            set(df_value_meta.histogram_system_metrics.keys())
            == expected_hist_sys_metrics
        )
        df_value_meta.histogram_system_metrics = None

        # assert df_value_meta.histograms == expected_value_meta.histograms
        # assert attr.asdict(df_value_meta) == attr.asdict(expected_value_meta)

        pandas_data_frame = spark_data_frame.toPandas()
        pandas_value_meta = DataFrameValueType().get_value_meta(
            pandas_data_frame, meta_conf
        )
        # assert df_value_meta == pandas_value_meta
