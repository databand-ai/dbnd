import attr

from dbnd._core.tracking.histograms import HistogramRequest, HistogramSpec
from dbnd_spark.spark_targets import SparkDataFrameValueType
from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values.pandas_values import DataFrameValueType


class TestSparkDataFrameValueType(object):
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
            "boolean_histograms_calc_time",
            "histograms_calc_time",
            "numerical_histograms_calc_time",
            "string_histograms_calc_time",
            "summary_calc_time",
        }

        meta_conf = ValueMetaConf.enabled()
        meta_conf.log_histograms = HistogramRequest.ALL()
        expected_value_meta = ValueMeta(
            value_preview=SparkDataFrameValueType().to_preview(
                spark_data_frame, meta_conf.get_preview_size()
            ),
            data_dimensions=(spark_data_frame.count(), len(spark_data_frame.columns)),
            data_hash=SparkDataFrameValueType().to_signature(spark_data_frame),
            data_schema=expected_data_schema,
            descriptive_stats=spark_data_frame_stats,
            histogram_spec=HistogramSpec(
                approx_distinct_count=False,
                columns=frozenset(spark_data_frame.columns),
                none=False,
                only_stats=False,
                with_stats=True,
            ),
            histograms=spark_data_frame_histograms,
        )

        df_value_meta = SparkDataFrameValueType().get_value_meta(
            spark_data_frame, meta_conf
        )

        assert df_value_meta.value_preview == expected_value_meta.value_preview
        assert df_value_meta.data_hash == expected_value_meta.data_hash
        assert df_value_meta.data_dimensions == expected_value_meta.data_dimensions
        assert df_value_meta.data_schema == expected_value_meta.data_schema
        assert df_value_meta.descriptive_stats == expected_value_meta.descriptive_stats
        assert df_value_meta.histogram_spec == expected_value_meta.histogram_spec

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
