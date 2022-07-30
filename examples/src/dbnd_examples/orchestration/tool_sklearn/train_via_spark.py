# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging

from typing import List, Tuple

import pyspark.sql as spark

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.sql.functions import exp
from pyspark.sql.types import DoubleType

from dbnd import log_metric, parameter, pipeline
from dbnd.utils import data_combine, period_dates
from dbnd_examples.data import demo_data_repo
from dbnd_spark.spark import spark_task
from targets import target
from targets.types import PathStr


logger = logging.getLogger(__name__)


def data_source(task_target_date, name):
    return target(demo_data_repo.partner_to_file(name, task_target_date))


@spark_task(result=parameter.output.csv[spark.DataFrame])
def get_and_enrich_spark(raw_data: spark.DataFrame, column_name: str):
    raw_data.show()
    data_with_new_feature = raw_data.withColumn(column_name + "_exp", exp(column_name))
    return data_with_new_feature


@spark_task(result=parameter.output.csv[spark.DataFrame])
def clean_data_spark(raw_data: spark.DataFrame):
    return raw_data.na.fill(0)


@pipeline()
def ingest_partner_a(task_target_date):
    raw_data = data_source(name="a", task_target_date=task_target_date)
    clean = clean_data_spark(raw_data=raw_data)
    return get_and_enrich_spark(raw_data=clean, column_name="1")


@pipeline
def ingest_partner_b(task_target_date):
    raw_data = data_source(name="b", task_target_date=task_target_date)
    return clean_data_spark(raw_data=raw_data)


@pipeline
def ingest_partner_c(task_target_date):
    raw_data = data_source(name="c", task_target_date=task_target_date)
    clean = clean_data_spark(raw_data=raw_data)
    return get_and_enrich_spark(raw_data=clean, column_name="10")


@pipeline
def fetch_partner_data_spark(
    task_target_date, selected_partners: List[str], period=datetime.timedelta(days=7)
) -> List[spark.DataFrame]:
    partner_data = []
    for partner in selected_partners:
        all_data = []
        for d in period_dates(task_target_date, period):
            if partner == "a":
                data = ingest_partner_a(task_target_date=d)
            elif partner == "b":
                data = ingest_partner_b(task_target_date=d)
            elif partner == "c":
                data = ingest_partner_c(task_target_date=d)
            else:
                raise Exception("Partner not found!")

            all_data.append(data)
        partner_data.append(data_combine(all_data, sort=True))
    return partner_data


@spark_task
def calculate_features(
    raw_data: List[spark.DataFrame], selected_features: List[str] = None
) -> spark.DataFrame:
    result = raw_data.pop(0)
    for d in raw_data:
        result = result.join(d, ["id"], "outer")
    if selected_features:
        result = result.select(selected_features)
    result.show()
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

    target_stats = raw_data.describe(["target"])

    log_metric(
        "target.mean",
        target_stats.filter(target_stats["summary"] == "mean")
        .collect()[0]
        .asDict()["target"],
    )
    log_metric(
        "target.std",
        target_stats.filter(target_stats["summary"] == "stddev")
        .collect()[0]
        .asDict()["target"],
    )

    return train, test, validation


@spark_task
def train_model_spark(
    test_set: spark.DataFrame,
    training_set: spark.DataFrame,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    saved_model=parameter.output.folder_data.with_flag(None)[PathStr],
) -> str:

    transform = VectorAssembler(inputCols=["0", "1", "2"], outputCol="features")
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="target",
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
        "label", prediction["target"].cast(DoubleType())
    ).select(["label", "prediction"])
    evaluation.show()
    metrics = RegressionMetrics(evaluation.rdd)

    log_metric("r2", metrics.r2)
    log_metric("alpha", alpha)

    model.write().save(str(saved_model))
    return "ok"


@pipeline(result=("model"))
def train_model_for_customer_spark(
    task_target_date,
    data: spark.DataFrame = None,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    period=datetime.timedelta(days=1),
    selected_features: List[str] = None,
):
    if data is None:
        partners = fetch_partner_data_spark(
            task_target_date=task_target_date, period=period
        )
        data = calculate_features(
            selected_features=selected_features, raw_data=partners
        )

    training_set, test_set, validation_set = split_data_spark(raw_data=data)

    model = train_model_spark(
        test_set=test_set, training_set=training_set, alpha=alpha, l1_ratio=l1_ratio
    )

    return model.saved_model
