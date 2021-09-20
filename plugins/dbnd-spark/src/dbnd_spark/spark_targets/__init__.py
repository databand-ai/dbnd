import pyspark

from dbnd_spark.spark_targets.spark_marshalling import (
    SparkDataFrameToCsv,
    SparkMarshaller,
)
from dbnd_spark.spark_targets.spark_values import SparkDataFrameValueType
from targets.marshalling import register_marshaller
from targets.target_config import FileFormat
from targets.values import register_value_type


def dbnd_register_spark_types():
    register_value_type(SparkDataFrameValueType())

    for file_format, marshaller in (
        (FileFormat.txt, SparkMarshaller(fmt=FileFormat.txt)),
        (FileFormat.csv, SparkDataFrameToCsv()),
        (FileFormat.json, SparkMarshaller(fmt=FileFormat.json)),
        (FileFormat.parquet, SparkMarshaller(fmt=FileFormat.parquet)),
    ):
        register_marshaller(pyspark.sql.DataFrame, file_format, marshaller)
