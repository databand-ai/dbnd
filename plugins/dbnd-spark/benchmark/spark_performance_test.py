import os

import click

from pyspark.sql import SparkSession

from dbnd import log_dataframe, task


@task
def histogram_test(input_file, app_name):
    app_name += "-" + os.path.basename(input_file)
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    data = spark.read.csv(input_file, inferSchema=True, header=True, sep=",")
    log_dataframe("data", data, with_histograms=True)
    spark.stop()


@click.command()
@click.option("--input", "-i", "input_file", help="input csv file")
@click.option("--name", "-n", "name", default="histogram_test", help="spark app name")
def main(input_file, name):
    if not input_file:
        # input_file = "s3://dbnd-dev-playground/data/booleans_100_million.csv"
        # input_file = "s3://dbnd-dev-playground/data/booleans_plusplus_5_million.csv"
        # input_file = "s3://dbnd-dev-playground/data/booleans_plusplus_10k.csv"
        input_file = "s3://dbnd-dev-playground/data/basic_1_million.csv"
    histogram_test(input_file, name)


if __name__ == "__main__":
    main()
