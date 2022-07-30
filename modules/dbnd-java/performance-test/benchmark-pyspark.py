# © Copyright Databand.ai, an IBM Company 2022

from pyspark.sql import SparkSession

from dbnd import log_metric, task


@task
def generate_report():
    spark = SparkSession.builder.appName("benchmark-pyspark").getOrCreate()

    df = spark.read.csv(
        "./datasets/backblaze-data-01gb", inferSchema=True, header=True, sep=","
    )
    df.select("serial_number").show(10)

    log_metric("records_count", 1)


generate_report()
