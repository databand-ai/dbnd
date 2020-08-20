import csv
import os
import time

from datetime import datetime

import click

from pyspark.sql import SparkSession

import dbnd

from dbnd import log_dataframe, task
from dbnd._core.utils.git import get_git_commit


def create_test_report(input_file, app_name, execution_time):
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    report_file = "reports/{}_{}".format(current_datetime, app_name)

    with open(report_file, "w", newline="") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(["key", "value"])
        writer.writerow(["input_file", input_file])
        writer.writerow(["spark_app_name", app_name])
        writer.writerow(["execution_time", round(execution_time, 3)])
        writer.writerow(["databand_commit", get_git_commit(dbnd.__file__)])
        writer.writerow(["datetime", current_datetime])


@task
def histogram_test(input_file, app_name):
    execution_time = None
    app_name += "-" + os.path.basename(input_file)
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    try:
        data = spark.read.csv(input_file, inferSchema=True, header=True, sep=",")
        start_time = time.time()
        log_dataframe("data", data, with_histograms=True)
        execution_time = time.time() - start_time
    finally:
        spark.stop()
        create_test_report(input_file, app_name, execution_time)


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
