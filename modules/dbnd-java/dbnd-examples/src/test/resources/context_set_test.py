# © Copyright Databand.ai, an IBM Company 2022

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min

from dbnd import dbnd_tracking, log_metric, task


@task
def parent_task(path):
    spark = SparkSession.builder.appName("report").getOrCreate()
    df = spark.read.csv(path, inferSchema=True, header=True)
    child_task(df)
    max_budget = df.agg(max("BUDGET_ON_EDUCATION")).collect()[0][0]
    log_metric("max_budget", max_budget)


@task
def child_task(df):
    min_gdp = df.agg(min("GDP")).collect()[0][0]
    log_metric("min_gdp", min_gdp)


# context_set_test.py <data_path> <job_name> <job_name2>
if __name__ == "__main__":
    with dbnd_tracking(sys.argv[2]):
        parent_task(sys.argv[1])
    # used to set context in the subsequent runs in the same Spark Session
    if len(sys.argv) == 4:
        with dbnd_tracking(sys.argv[3]):
            parent_task(sys.argv[1])
    print("\n\nScript finished\n\n")
