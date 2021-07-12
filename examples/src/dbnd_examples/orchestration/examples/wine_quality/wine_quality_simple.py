import logging

from typing import Tuple

import numpy as np

from pandas import DataFrame
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from dbnd import log_dataframe, log_metric, pipeline, task


logging.basicConfig(level=logging.INFO)


@task(result="training_set, validation_set")
def prepare_data(raw_data: DataFrame) -> Tuple[DataFrame, DataFrame]:
    train_df, validation_df = train_test_split(raw_data)

    return train_df, validation_df


@task
def train_model(
    training_set: DataFrame, alpha: float = 0.5, l1_ratio: float = 0.5,
) -> ElasticNet:
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
    lr.fit(training_set.drop(["quality"], 1), training_set[["quality"]])

    return lr


@task
def validate_model(model: ElasticNet, validation_dataset: DataFrame) -> str:
    log_dataframe("validation", validation_dataset)

    validation_x = validation_dataset.drop(["quality"], 1)
    validation_y = validation_dataset[["quality"]]

    prediction = model.predict(validation_x)
    rmse = np.sqrt(mean_squared_error(validation_y, prediction))
    mae = mean_absolute_error(validation_y, prediction)
    r2 = r2_score(validation_y, prediction)

    log_metric("rmse", rmse)
    log_metric("mae", rmse)
    log_metric("r2", r2)

    return "%s,%s,%s" % (rmse, mae, r2)


@pipeline(result=("model", "validation"))
def predict_wine_quality(
    raw_data: DataFrame, alpha: float = 0.5, l1_ratio: float = 0.5,
):
    training_set, validation_set = prepare_data(raw_data=raw_data)

    model = train_model(training_set=training_set, alpha=alpha, l1_ratio=l1_ratio)
    validation = validate_model(model=model, validation_dataset=validation_set)

    return model, validation
