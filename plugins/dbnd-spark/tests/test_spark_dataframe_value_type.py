import pytest

from dbnd_spark.spark_targets import SparkDataFrameValueType
from targets.value_meta import ValueMeta, ValueMetaConf
from targets.values.pandas_values import DataFrameValueType


class TestSparkDataFrameValueType(object):
    @pytest.mark.skip
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

        meta_conf = ValueMetaConf.enabled()
        expected_value_meta = ValueMeta(
            value_preview=SparkDataFrameValueType().to_preview(
                spark_data_frame, meta_conf.get_preview_size()
            ),
            data_dimensions=(spark_data_frame.count(), len(spark_data_frame.columns)),
            data_schema=expected_data_schema,
            data_hash=SparkDataFrameValueType().to_signature(spark_data_frame),
            descriptive_stats=spark_data_frame_stats,
            histograms=spark_data_frame_histograms,
        )

        df_value_meta = SparkDataFrameValueType().get_value_meta(
            spark_data_frame, meta_conf
        )

        assert df_value_meta.value_preview == expected_value_meta.value_preview
        assert df_value_meta.data_hash == expected_value_meta.data_hash
        assert df_value_meta.data_schema == expected_value_meta.data_schema
        assert df_value_meta.data_dimensions == expected_value_meta.data_dimensions
        # assert df_value_meta == expected_value_meta

        pandas_data_frame = spark_data_frame.toPandas()
        pandas_value_meta = DataFrameValueType().get_value_meta(
            pandas_data_frame, meta_conf
        )
        # assert df_value_meta == pandas_value_meta
