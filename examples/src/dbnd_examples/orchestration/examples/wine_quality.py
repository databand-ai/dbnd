# This example requires pandas, numpy, sklearn, scipy
# Inspired by an MLFlow tutorial:
#   https://github.com/databricks/mlflow/blob/master/example/tutorial/train.py
import datetime
import itertools
import logging
import sys

from typing import Tuple

import numpy as np
import pandas as pd

from pandas import DataFrame
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from dbnd import (
    dbnd_config,
    dbnd_handle_errors,
    log_dataframe,
    log_metric,
    output,
    parameter,
    pipeline,
    task,
)
from dbnd.utils import data_combine, period_dates
from dbnd_examples.data import data_repo
from targets import target
from targets.types import PathStr


logger = logging.getLogger(__name__)


# dbnd run dbnd_examples.orchestration.examples.wine_quality.predict_wine_quality --task-version now
# dbnd run dbnd_examples.orchestration.examples.wine_quality.predict_wine_quality_parameter_search --task-version now


def calculate_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2


@task(result="training_set, test_set, validation_set")
def prepare_data(raw_data: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Split data into train, test and validation"""
    train_df, test_df = train_test_split(raw_data)
    test_df, validation_df = train_test_split(test_df, test_size=0.5)

    sys.stderr.write("Running Prepare Data! You'll see this message in task log \n")
    print("..and this one..\n")
    logger.info("..and this one for sure!")

    log_dataframe("raw", raw_data, with_histograms=True)

    return train_df, test_df, validation_df


@task
def calculate_alpha(alpha: float = 0.5) -> float:
    """Calculates alpha for train_model"""
    alpha += 0.1
    return alpha


@task(training_set=parameter[DataFrame](log_histograms=True, log_stats=True))
def train_model(
    test_set: DataFrame,
    training_set: DataFrame,
    alpha: float = 0.5,
    l1_ratio: float = 0.5,
) -> ElasticNet:
    """Train wine prediction model"""
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
    lr.fit(training_set.drop(["quality"], 1), training_set[["quality"]])
    prediction = lr.predict(test_set.drop(["quality"], 1))

    (rmse, mae, r2) = calculate_metrics(test_set[["quality"]], prediction)

    log_metric("alpha", alpha)
    log_metric("rmse", rmse)
    log_metric("mae", rmse)
    log_metric("r2", r2)

    logging.info(
        "Elasticnet model (alpha=%f, l1_ratio=%f): rmse = %f, mae = %f, r2 = %f",
        alpha,
        l1_ratio,
        rmse,
        mae,
        r2,
    )
    return lr


@task
def validate_model(model: ElasticNet, validation_dataset: DataFrame) -> str:
    """Calculates metrics of wine prediction model"""
    log_dataframe("validation", validation_dataset)
    # support for py3 parqeut
    validation_dataset = validation_dataset.rename(str, axis="columns")
    validation_x = validation_dataset.drop(["quality"], 1)
    validation_y = validation_dataset[["quality"]]

    prediction = model.predict(validation_x)
    (rmse, mae, r2) = calculate_metrics(validation_y, prediction)

    log_metric("rmse", rmse)
    log_metric("mae", rmse)
    log_metric("r2", r2)

    return "%s,%s,%s" % (rmse, mae, r2)


@pipeline(result=("model", "validation"))
def predict_wine_quality(
    data: DataFrame = None,
    alpha: float = 0.5,
    l1_ratio: float = 0.5,
    good_alpha: bool = False,
):
    """Entry point for wine quality prediction"""
    if data is None:
        data = fetch_data()
    training_set, test_set, validation_set = prepare_data(raw_data=data)

    if good_alpha:
        alpha = calculate_alpha(alpha)

    model = train_model(
        test_set=test_set, training_set=training_set, alpha=alpha, l1_ratio=l1_ratio
    )
    validation = validate_model(model=model, validation_dataset=validation_set)
    return model, validation


@pipeline
def predict_wine_quality_parameter_search(
    alpha_step: float = 0.3, l1_ratio_step: float = 0.4
):
    result = {}
    variants = list(
        itertools.product(np.arange(0, 1, alpha_step), np.arange(0, 1, l1_ratio_step))
    )
    logger.info("All Variants: %s", variants)
    for alpha_value, l1_ratio in variants:
        exp_name = "Predict_%f_l1_ratio_%f" % (alpha_value, l1_ratio)
        model, validation = predict_wine_quality(
            alpha=alpha_value, l1_ratio=l1_ratio, task_name=exp_name
        )

        result[exp_name] = (model, validation)
    return result


# DATA FETCHING
@pipeline
def wine_quality_day(
    task_target_date: datetime.date, root_location: PathStr = data_repo.wines_per_date
) -> pd.DataFrame:
    return target(root_location, task_target_date.strftime("%Y-%m-%d"), "wine.csv")


@task(result=output.prod_immutable[DataFrame](log_histograms=True))
def fetch_wine_quality(
    task_target_date: datetime.date, data: pd.DataFrame = data_repo.wines_full
) -> pd.DataFrame:
    # very simple implementation that just sampe the data with seed = target date
    return DataFrame.sample(data, frac=0.2, random_state=task_target_date.day)


@pipeline
def fetch_data(task_target_date, period=datetime.timedelta(days=7)):
    all_data = []
    for d in period_dates(task_target_date, period):
        data = fetch_wine_quality(task_target_date=d)
        all_data.append(data)

    return data_combine(all_data, sort=True)


def main():
    # NATIVE EXECUTION
    print(predict_wine_quality(pd.read_csv(data_repo.wines)))


@dbnd_handle_errors()
def main_dbnd():
    # DBND EXECUTION
    with dbnd_config({fetch_data.task.period: "2d"}):
        predict_wine_quality.dbnd_run()


if __name__ == "__main__":
    main()
