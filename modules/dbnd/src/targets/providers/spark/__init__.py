# © Copyright Databand.ai, an IBM Company 2022

from targets.marshalling import MarshallerLoader
from targets.target_config import FileFormat
from targets.values import ValueTypeLoader, register_value_type


def register_value_types_spark():
    value_type_spark_df = register_value_type(
        ValueTypeLoader(
            "pyspark.sql.DataFrame",
            "targets.providers.spark.spark_values.SparkDataFrameValueType",
            "dbnd",
            type_str_extras=["spark.DataFrame", "pyspark.sql.dataframe.DataFrame"],
        )
    )

    value_type_spark_df.register_marshallers(
        {
            FileFormat.txt: MarshallerLoader(
                "targets.providers.spark.spark_marshalling.SparkMarshallerTxt",
                package="dbnd",
            ),
            FileFormat.csv: MarshallerLoader(
                "targets.providers.spark.spark_marshalling.SparkDataFrameToCsv",
                package="dbnd",
            ),
            FileFormat.json: MarshallerLoader(
                "targets.providers.spark.spark_marshalling.SparkMarshallerJson",
                package="dbnd",
            ),
            FileFormat.parquet: MarshallerLoader(
                "targets.providers.spark.spark_marshalling.SparkMarshallerParquet",
                package="dbnd",
            ),
        }
    )
