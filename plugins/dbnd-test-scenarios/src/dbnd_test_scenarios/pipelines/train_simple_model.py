import datetime
import logging
import pickle
import sys

from typing import List, Tuple

import numpy as np
import pandas as pd

from matplotlib import figure
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from dbnd import log_dataframe, log_metric, task


logger = logging.getLogger(__name__)


def calculate_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2


@task
def split_data(
    raw_data: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    columns_to_remove = set(["id", "1_norm", "10_norm"])
    if columns_to_remove.issubset(raw_data.columns):
        raw_data.drop(columns_to_remove, axis=1, inplace=True)

    train_df, test_df = train_test_split(raw_data)
    test_df, validation_df = train_test_split(test_df, test_size=0.5)

    log_dataframe("raw", raw_data)
    log_metric("target.mean", raw_data["target"].mean())
    log_metric("target.std", raw_data["target"].std())

    return train_df, test_df, validation_df


@task
def train_model(
    test_set: pd.DataFrame,
    training_set: pd.DataFrame,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
) -> ElasticNet:
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
    lr.fit(training_set.drop(["target"], 1), training_set[["target"]])
    prediction = lr.predict(test_set.drop(["target"], 1))

    (rmse, mae, r2) = calculate_metrics(test_set[["target"]], prediction)

    log_metric("rmse", rmse)
    log_metric("mae", mae)
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


def _create_scatter_plot(actual, predicted) -> figure.Figure:
    import matplotlib.pyplot as plt

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.set_title("Actual vs. Predicted")
    ax.set_xlabel("Actual Labels")
    ax.set_ylabel("Predicted Values")
    ax.scatter(actual, predicted)
    return fig


@task(result=("report", "prediction_scatter_plot"))
def validate_model_for_customer(
    model: ElasticNet, validation_dataset: pd.DataFrame, threshold=0.2
) -> Tuple[str, figure.Figure]:
    log_dataframe("validation", validation_dataset)
    # support for py3 parqeut
    validation_dataset = validation_dataset.rename(str, axis="columns")
    validation_x = validation_dataset.drop(["target"], 1)
    validation_y = validation_dataset[["target"]]

    prediction = model.predict(validation_x)
    (rmse, mae, r2) = calculate_metrics(validation_y, prediction)

    log_metric("rmse", rmse)
    log_metric("mae", mae)
    log_metric("r2", r2)
    fig = _create_scatter_plot(validation_y, prediction)
    if r2 < threshold:
        raise Exception(
            "Model quality is below threshold. Got R2 equal to %s, expect at least %s"
            % (r2, threshold)
        )

    return "%s,%s,%s" % (rmse, mae, r2), fig


@task
def train_model_for_customer(
    data: pd.DataFrame = None, alpha: float = 1.0, l1_ratio: float = 0.5
):

    training_set, test_set, validation_set = split_data(raw_data=data)

    model = train_model(
        test_set=test_set, training_set=training_set, alpha=alpha, l1_ratio=l1_ratio
    )
    validation, _ = validate_model_for_customer(
        model=model, validation_dataset=validation_set
    )
    return model, validation


if __name__ == "__main__":
    model, validation = train_model_for_customer(pd.read_csv(sys.argv[1]))
    with open(sys.argv[2], "wb") as f:
        pickle.dump(model, f)
    print("Ok")
