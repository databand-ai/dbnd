from typing import Tuple

import time
import random

from pyspark.sql import DataFrame, SparkSession

from dbnd import task, log_dataframe, log_metric


@task
def unit_imputations(
        raw_data: DataFrame,
        value: int
) -> DataFrame:
    counter = int(raw_data.describe().first().phone)
    noise = random.randint(-counter, counter)

    log_metric("Replaced NaNs", counter + noise)

    return raw_data.na.fill(value)


@task
def dedup_records(
        data: DataFrame,
        key_columns: list,
        to_pandas: bool,
        with_histograms: bool,
        sampling_type: str,
        sampling_fraction: float
) -> Tuple[DataFrame, tuple]:
    data = data.dropDuplicates(key_columns)

    log_data = data
    if sampling_type is not None:
        if sampling_type == 'random':
            log_data = log_data.sample(False, sampling_fraction)
        if sampling_type == 'first':
            log_data = log_data.limit(int(log_data.count() * sampling_fraction))

    inputs_shape = (log_data.count(), len(log_data.columns))

    if to_pandas:
        log_data = log_data.toPandas()

    log_dataframe("data", log_data, with_histograms=with_histograms)

    return data, inputs_shape


@task
def create_report(data: DataFrame) -> DataFrame:
    log_metric("Column Count", len(data.columns))
    log_metric(
        "Avg Score",
        int(data.agg({"score": "sum"}).collect()[0][0] + random.randint(-2 * len(data.columns), 2 * len(data.columns))),
    )

    return data


@task
def augment_data(data: DataFrame, multiplicator: int) -> DataFrame:
    for i in range(multiplicator - 1):
        for column in data.columns:
            data = data.withColumn(f'{column}_{i}', data[column])
    return data


@task
def process_customer_data(
        app_name: str,
        input_file: str,
        output_file: str,
        to_pandas: bool,
        with_histograms: bool,
        sampling_type: str,
        sampling_fraction: float,
        columns_number_multiplicator: int
) -> Tuple[str, tuple]:
    key_columns = ["name"]

    spark = (
        SparkSession
            .builder
            .appName(app_name)
            .master("yarn")
            .config("spark.submit.deployMode", "client")
            .config("spark.executor.memory", "4g")
            .config("spark.driver.memory", "10g")
            .getOrCreate()
    )

    app_id = spark._jsc.sc().applicationId()

    data = spark.read.csv(input_file, inferSchema=True, header=True, sep=",")

    data = augment_data(data, columns_number_multiplicator)
    imputed = unit_imputations(data, value=0)
    clean, inputs_shape = dedup_records(
        imputed, key_columns, to_pandas, with_histograms, sampling_type, sampling_fraction
    )
    report = create_report(clean)

    report.write.csv(f"{output_file}/{str(round(time.time()))}")

    spark.stop()

    return app_id, inputs_shape
