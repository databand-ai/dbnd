# © Copyright Databand.ai, an IBM Company 2022

import hashlib
import logging
import sys

from random import randint

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf

from dbnd import log_dataframe, log_metric, task, track_functions


logger = logging.getLogger(__name__)


def get_hash(value: str) -> str:
    return hashlib.md5(value.encode()).hexdigest()  # nosec B324


get_hash_udf = udf(get_hash)


# with type annotations
def unit_imputation(
    raw_data: DataFrame, columns_to_impute=["10"], value=0
) -> DataFrame:
    counter = int(raw_data.describe().first().phone)
    noise = randint(-counter, counter)
    log_metric("Replaced NaNs", counter + noise)
    return raw_data.na.fill(value, columns_to_impute)


# without type annotations
def dedup_records(data: DataFrame, key_columns=["name"]) -> DataFrame:
    data = data.dropDuplicates(key_columns)
    log_dataframe("data", data, with_histograms=True)
    return data


def create_report(data: DataFrame) -> DataFrame:
    log_metric("Column Count", len(data.columns))
    log_metric(
        "Avg Score",
        int(
            data.agg({"score": "sum"}).collect()[0][0]
            + randint(-2 * len(data.columns), 2 * len(data.columns))
        ),
    )
    return data


# Tracking - option 2
track_functions(dedup_records, unit_imputation, create_report)


@task
def process_customer_data(input_file: str, output_file: str) -> None:
    key_columns = ["name"]
    columns_to_impute = ["10"]

    spark = SparkSession.builder.appName("ingest_data_spark").getOrCreate()
    data = spark.read.csv(input_file, inferSchema=True, header=True, sep=",")

    imputed = unit_imputation(data, columns_to_impute)
    clean = dedup_records(imputed, key_columns)
    report = create_report(clean)
    report.write.csv(output_file)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output>")
        sys.exit(-1)
    process_customer_data(sys.argv[1], sys.argv[2])
