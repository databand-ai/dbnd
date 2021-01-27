import logging

from typing import List, Tuple

import pyspark.sql as spark

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import exp, udf
from pyspark.sql.types import DoubleType

from dbnd import log_metric, output, parameter, task
from dbnd._core.constants import DbndTargetOperationType
from dbnd._core.tracking.metrics import log_dataframe
from dbnd._vendor import click
from dbnd_spark.spark import spark_task
from dbnd_test_scenarios.data_chaos_monkey.chaos_utils import chaos_float, chaos_int
from dbnd_test_scenarios.scenarios_repo import client_scoring_data
from dbnd_test_scenarios.utils.data_utils import get_hash
from targets.types import PathStr


logger = logging.getLogger(__name__)

LABEL_COLUMN = "score_label"
SELECTED_FEATURES = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]
get_hash_udf = udf(get_hash)
model_output_parameter = parameter.output.folder_data.pickle.with_flag(None)[PathStr]


# with type annotations
@spark_task
def unit_imputation(
    raw_data: DataFrame, columns_to_impute=["10"], value=0
) -> DataFrame:
    log_metric("Replaced NaNs", chaos_int(0.5 * raw_data.count()))

    return raw_data.na.fill(value, columns_to_impute)


# without type annotations   (log_histograms=True)
@spark_task(result=output[DataFrame])
def dedup_records(data: DataFrame, key_columns) -> DataFrame:
    data = data.dropDuplicates(key_columns)

    data_with_new_feature = data.withColumn("10_exp", exp("10"))
    return data_with_new_feature


@spark_task
def create_report(data: DataFrame) -> DataFrame:
    log_metric("Column Count", len(data.columns))
    log_dataframe("ready_data", data, with_histograms=True)
    avg_score = data.agg({"score_label": "sum"}).collect()[0][0]
    log_metric("Avg Score", chaos_float(avg_score))
    return data


@spark_task
def ingest_customer_data(data: spark.DataFrame) -> spark.DataFrame:
    key_columns = ["id", "10"]
    columns_to_impute = ["10"]
    data = data.repartition(1)
    imputed = unit_imputation(data, columns_to_impute)
    clean = dedup_records(imputed, key_columns)
    report = create_report(clean)
    return clean


@spark_task
def join(raw_data: List[spark.DataFrame]):
    result = raw_data.pop(0)
    for d in raw_data:
        result = result.join(d, ["id"], "outer")
    return result


@spark_task
def calculate_features(
    data: spark.DataFrame,
    selected_features: List[str] = None,
    columns_to_remove: List[str] = None,
) -> spark.DataFrame:
    if columns_to_remove:
        for col in columns_to_remove:
            if data.columns.contains(col):
                data = data.drop([col])
    if selected_features:
        data = data.select(selected_features)
    return data


@spark_task(result="training_set, test_set, validation_set")
def split_data_spark(
    raw_data: spark.DataFrame,
) -> Tuple[spark.DataFrame, spark.DataFrame, spark.DataFrame]:
    (train, test) = raw_data.randomSplit([0.8, 0.2])
    (test, validation) = raw_data.randomSplit([0.5, 0.5])
    return train, test, validation


@spark_task
def train_model_spark(
    test_set: parameter(log_histograms=True)[spark.DataFrame],
    training_set: spark.DataFrame,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    saved_model=model_output_parameter,
) -> str:
    transform = VectorAssembler(inputCols=SELECTED_FEATURES, outputCol="features")
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
        "label", prediction["score_label"].cast(DoubleType())
    ).select(["label", "prediction"])
    evaluation.show()
    metrics = RegressionMetrics(evaluation.rdd)

    log_metric("r2", metrics.r2)
    log_metric("alpha", alpha)

    path = str(saved_model)
    model.write().save(path)
    return path


@spark_task(result=("model"))
def train_model_for_customer_spark(
    data: spark.DataFrame,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    selected_features: List[str] = None,
    saved_model=model_output_parameter,
):
    selected_features = selected_features or SELECTED_FEATURES
    if LABEL_COLUMN not in selected_features:
        selected_features = selected_features + [LABEL_COLUMN]
    data = calculate_features(data=data, selected_features=selected_features)

    training_set, test_set, validation_set = split_data_spark(raw_data=data)

    model = train_model_spark(
        test_set=test_set,
        training_set=training_set,
        alpha=alpha,
        l1_ratio=l1_ratio,
        saved_model=saved_model,
    )

    return model


@click.group()
def cli():
    pass


@cli.command(name="ingest")
@click.option("--data", default=client_scoring_data.p_g_ingest_data, help="Ingst data")
@click.option(
    "--output-path", default="/tmp/ingested.csv", help="Output model location"
)
@task
def run_ingest(data: PathStr, output_path: PathStr):
    spark = SparkSession.builder.appName("ingest_data_spark").getOrCreate()
    data_df = spark.read.csv(data, inferSchema=True, header=True, sep=",")

    result_df = ingest_customer_data(data_df)

    log_dataframe(
        "published",
        result_df,
        path=output_path,
        with_histograms=True,
        operation_type=DbndTargetOperationType.write,
    )
    result_df.write.csv(str(output_path), header=True)
    spark.stop()


@cli.command(name="train")
@click.option(
    "--data", default=client_scoring_data.p_g_train_data, help="Training data"
)
@click.option(
    "--model-path", default="/tmp/trained_model.pickle", help="Output model location"
)
@task
def run_train(data: PathStr, model_path: PathStr):
    spark = SparkSession.builder.appName("train_data_spark").getOrCreate()
    data_df = spark.read.csv(data, inferSchema=True, header=True, sep=",")

    log_dataframe("ingest", data_df, path=data, with_histograms=True)
    train_model_for_customer_spark(data_df, saved_model=model_path)
    spark.stop()


if __name__ == "__main__":
    cli()
