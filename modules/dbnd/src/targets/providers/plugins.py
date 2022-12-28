# © Copyright Databand.ai, an IBM Company 2022

from targets.marshalling import MarshallerLoader
from targets.target_config import FileFormat
from targets.values import ValueTypeLoader, register_value_type


def register_value_types_from_plugins():
    # Plugin dbnd-spark
    value_type_spark_df = register_value_type(
        ValueTypeLoader(
            "pyspark.sql.DataFrame",
            "dbnd_spark.spark_targets.spark_values.SparkDataFrameValueType",
            "dbnd-spark",
            type_str_extras=["spark.DataFrame", "pyspark.sql.dataframe.DataFrame"],
        )
    )

    value_type_spark_df.register_marshallers(
        {
            FileFormat.txt: MarshallerLoader(
                "dbnd_spark.spark_targets.spark_marshalling.SparkMarshallerTxt",
                package="dbnd-spark",
            ),
            FileFormat.csv: MarshallerLoader(
                "dbnd_spark.spark_targets.spark_marshalling.SparkDataFrameToCsv",
                package="dbnd-spark",
            ),
            FileFormat.json: MarshallerLoader(
                "dbnd_spark.spark_targets.spark_marshalling.SparkMarshallerJson",
                package="dbnd-spark",
            ),
            FileFormat.parquet: MarshallerLoader(
                "dbnd_spark.spark_targets.spark_marshalling.SparkMarshallerParquet)",
                package="dbnd-spark",
            ),
        }
    )

    # Plugin dbnd-tensorflow
    tfm = register_value_type(
        ValueTypeLoader(
            "tensorflow.keras.models.Model",
            "dbnd_tensorflow.marshalling.tensorflow_values.TensorflowModelValueType",
            "dbnd-tensorflow",
        )
    )
    tfm.register_marshaller(
        FileFormat.tfmodel,
        MarshallerLoader(
            "dbnd_tensorflow.marshalling.tensorflow_marshaller.TensorflowKerasModelMarshaller"
        ),
    )

    tf_history = register_value_type(
        ValueTypeLoader(
            "tensorflow.keras.callbacks.History",
            "dbnd_tensorflow.marshalling.tensorflow_values.TensorflowHistoryValueType",
            "dbnd-tensorflow",
        )
    )
    tf_history.register_marshaller(
        FileFormat.tfhistory,
        MarshallerLoader(
            "dbnd_tensorflow.marshalling.tensorflow_marshaller.TensorflowKerasHistoryMarshaller",
            "dbnd-tensorflow",
        ),
    )
