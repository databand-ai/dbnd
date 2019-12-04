from dbnd._core.commands.metrics import log_artifact, log_dataframe, log_metric


def get_spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()
