import logging

from dbnd._core.utils.seven import import_errors
from dbnd_spark.targets.spark_marshalling import SparkMarshaller, SparkDataFrameToCsv
from dbnd_spark.targets.spark_values import SparkDataFrameValueType
from targets.marshalling import register_marshaller
from targets.target_config import FileFormat
from targets.values import register_value_type

logger = logging.getLogger(__name__)

_DBND_REGISTER_SPARK_TYPES = None


def dbnd_register_spark_types():
    global _DBND_REGISTER_SPARK_TYPES
    if _DBND_REGISTER_SPARK_TYPES:
        return
    _DBND_REGISTER_SPARK_TYPES = True
    try:
        import pyspark
    except import_errors as ex:
        # pyspark is not installed, probably user will not use pyspark types
        return

    try:
        register_value_type(SparkDataFrameValueType())

        for file_format, marshaller in (
            (FileFormat.txt, SparkMarshaller(fmt=FileFormat.txt)),
            (FileFormat.csv, SparkDataFrameToCsv()),
            (FileFormat.json, SparkMarshaller(fmt=FileFormat.json)),
            (FileFormat.parquet, SparkMarshaller(fmt=FileFormat.parquet)),
        ):
            register_marshaller(pyspark.sql.DataFrame, file_format, marshaller)
    except Exception:
        logger.info("Failed to register pyspark types")
