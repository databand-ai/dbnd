from dbnd._core.utils import json_utils
from dbnd_spark.targets import SparkDataFrameValueType
from targets.target_meta import TargetMeta


class TestSparkDataFrameValueType(object):
    def test_spark_df_value_meta(self, spark_data_frame):
        expected_data_schema = {
            "type": SparkDataFrameValueType.type_str,
            "columns": list(spark_data_frame.schema.names),
            "size": int(spark_data_frame.count() * len(spark_data_frame.columns)),
            "shape": (spark_data_frame.count(), len(spark_data_frame.columns)),
            "dtypes": {f.name: str(f.dataType) for f in spark_data_frame.schema.fields},
        }

        expected_value_meta = TargetMeta(
            value_preview=SparkDataFrameValueType().to_preview(spark_data_frame),
            data_dimensions=(spark_data_frame.count(), len(spark_data_frame.columns)),
            data_schema=json_utils.dumps(expected_data_schema),
            data_hash=None,
        )

        df_value_meta = SparkDataFrameValueType().get_value_meta(spark_data_frame)

        assert df_value_meta.value_preview == expected_value_meta.value_preview
        assert df_value_meta.data_hash == expected_value_meta.data_hash
        assert df_value_meta.data_schema == expected_value_meta.data_schema
        assert df_value_meta.data_dimensions == expected_value_meta.data_dimensions
        assert df_value_meta == expected_value_meta
