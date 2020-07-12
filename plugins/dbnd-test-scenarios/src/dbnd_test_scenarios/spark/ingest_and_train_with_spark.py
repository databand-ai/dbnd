import logging

from random import randint
from typing import List, Tuple

import pyspark.sql as spark

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.sql import DataFrame
from pyspark.sql.functions import exp, udf
from pyspark.sql.types import DoubleType

from dbnd import log_metric, output, parameter, pipeline
from dbnd_spark.spark import spark_task
from dbnd_test_scenarios.pipelines.client_scoring.ingest_data import (
    partner_file_data_location,
)
from dbnd_test_scenarios.utils.data_utils import get_hash
from targets.types import PathStr


logger = logging.getLogger(__name__)

LABEL_COLUMN = "score"

get_hash_udf = udf(get_hash)


# with type annotations
@spark_task
def unit_imputation(
    raw_data: DataFrame, columns_to_impute=["10"], value=0
) -> DataFrame:
    counter = int(raw_data.describe().first().phone)
    noise = randint(-counter, counter)
    log_metric("Replaced NaNs", counter + noise)
    return raw_data.na.fill(value, columns_to_impute)


# without type annotations
@spark_task(result=output(log_histograms=True)[DataFrame])
def dedup_records(data: DataFrame, key_columns=["name"]) -> DataFrame:
    data = data.dropDuplicates(key_columns)
    return data


@spark_task
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


@spark_task
def ingest_customer_data(data: spark.DataFrame) -> spark.DataFrame:
    key_columns = ["name"]
    columns_to_impute = ["10"]

    imputed = unit_imputation(data, columns_to_impute)
    clean = dedup_records(imputed, key_columns)
    report = create_report(clean)
    return report


@spark_task(result=parameter.output.csv[spark.DataFrame])
def get_and_enrich_spark(raw_data: spark.DataFrame, column_name: str):
    raw_data.show()
    data_with_new_feature = raw_data.withColumn(column_name + "_exp", exp(column_name))
    return data_with_new_feature


@spark_task(result=parameter.output.csv[spark.DataFrame])
def clean_data_spark(raw_data: spark.DataFrame):
    return raw_data.na.fill(0)


@pipeline
def ingest_partner_data(task_target_date):
    raw_data = partner_file_data_location(name="a", task_target_date=task_target_date)
    clean = clean_data_spark(raw_data=raw_data)
    return get_and_enrich_spark(raw_data=clean, column_name="1")


@spark_task
def calculate_features(
    raw_data: List[spark.DataFrame], selected_features: List[str] = None
) -> spark.DataFrame:
    result = raw_data.pop(0)
    for d in raw_data:
        result = result.join(d, ["id"], "outer")
    if selected_features:
        result = result.select(selected_features)
    return result


@spark_task(result="training_set, test_set, validation_set")
def split_data_spark(
    raw_data: spark.DataFrame,
) -> Tuple[spark.DataFrame, spark.DataFrame, spark.DataFrame]:

    columns_to_remove = set(["id", "0_norm", "10_norm"])
    if columns_to_remove.issubset(list(raw_data.schema.names)):
        raw_data = raw_data.drop(columns_to_remove)

    (train, test) = raw_data.randomSplit([0.8, 0.2])
    (test, validation) = raw_data.randomSplit([0.5, 0.5])

    return train, test, validation


@spark_task
def train_model_spark(
    test_set: parameter(log_histograms=True)[spark.DataFrame],
    training_set: spark.DataFrame,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    saved_model=parameter.output.folder_data.with_flag(None)[PathStr],
) -> str:

    transform = VectorAssembler(inputCols=["0", "1", "2"], outputCol="features")
    lr = LogisticRegression(
        featuresCol="features",
        labelCol=LABEL_COLUMN,
        regParam=l1_ratio,
        elasticNetParam=alpha,
        family="multinomial",
        maxIter=1,
    )
    ppl = Pipeline(stages=[transform, lr])

    # Fit the pipeline to training documents.
    model = ppl.fit(training_set)

    prediction = model.transform(test_set)
    evaluation = prediction.withColumn(
        "label", prediction["score"].cast(DoubleType())
    ).select(["label", "prediction"])
    evaluation.show()
    metrics = RegressionMetrics(evaluation.rdd)

    log_metric("r2", metrics.r2)
    log_metric("alpha", alpha)

    model.write().save(str(saved_model))
    return "ok"


@spark_task(result=("model"))
def train_model_for_customer_spark(
    data: List[spark.DataFrame],
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    selected_features: List[str] = None,
):
    data = calculate_features(selected_features=selected_features, raw_data=data)

    training_set, test_set, validation_set = split_data_spark(raw_data=data)

    model = train_model_spark(
        test_set=test_set, training_set=training_set, alpha=alpha, l1_ratio=l1_ratio
    )

    return model.saved_model
