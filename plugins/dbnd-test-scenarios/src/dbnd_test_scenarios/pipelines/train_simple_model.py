import datetime
import itertools
import logging
import pickle
import sys

from typing import List, Tuple

import matplotlib
import numpy as np
import pandas as pd

from matplotlib import figure
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from dbnd import current, log_dataframe, log_metric, parameter, pipeline, task
from dbnd_test_scenarios.pipelines.ingest_train import fetch_partner_data


logger = logging.getLogger(__name__)
matplotlib.use("Agg")

logger = logging.getLogger(__name__)


# Utilities
def calculate_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)

    log_metric("rmse", rmse)
    log_metric("mae", mae)
    log_metric("r2", r2)
    return rmse, mae, r2


@task
def calculate_features(
    raw_data: List[pd.DataFrame], selected_features: List[str] = None
) -> pd.DataFrame:
    result = raw_data.pop(0)
    for d in raw_data:
        result = result.merge(d, on="id")
    if selected_features:
        result = result[selected_features]
    return result


@task(result="training_set, test_set, validation_set")
def split_data(
    raw_data: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw_data.drop(["id", "1_norm", "10_norm"], axis=1, inplace=True, errors="ignore")

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


@task
def create_scatter_plot(actual, predicted) -> figure.Figure:
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

    fig = create_scatter_plot(validation_y, prediction)
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


@pipeline(result=("model", "validation"))
def train_model_for_customer_data_validation(
    task_target_date,
    data: pd.DataFrame = None,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    period=datetime.timedelta(days=7),
    selected_features: List[str] = None,
    validate_data=False,
):
    if data is None:
        partners = fetch_partner_data(task_target_date=task_target_date, period=period)
        data = calculate_features(
            selected_features=selected_features, raw_data=partners
        )

    split = split_data.t(raw_data=data)
    if not validate_data:

        split.set_upstream(data_anomalies.task)

    model = train_model(
        test_set=split.test_set,
        training_set=split.training_set,
        alpha=alpha,
        l1_ratio=l1_ratio,
    )
    validation, _ = validate_model_for_customer(
        model=model, validation_dataset=split.validation_set
    )
    return model, validation


@pipeline
def train_for_all_customer(customer=parameter[List[pd.DataFrame]]):
    result = {}
    for c in customer:
        (model, validation) = train_model_for_customer(data=c)
        result[str(c)] = (model, validation)
    return result


@pipeline
def training_with_parameter_search(
    data: pd.DataFrame = partner_demo_data.sample_customer_file,
    alpha_step: float = 0.3,
    l1_ratio_step: float = 0.4,
):
    result = {}
    variants = list(
        itertools.product(np.arange(0, 1, alpha_step), np.arange(0, 1, l1_ratio_step))
    )
    logger.info("All Variants: %s", variants)
    for alpha_value, l1_ratio in variants:
        exp_name = "Predict_%f_l1_ratio_%f" % (alpha_value, l1_ratio)
        model, validation = train_model_for_customer(
            data=data, alpha=alpha_value, l1_ratio=l1_ratio, task_name=exp_name
        )

        result[exp_name] = (model, validation)
    return result


if __name__ == "__main__":
    model, validation = train_model_for_customer(pd.read_csv(sys.argv[1]))
    with open(sys.argv[2], "wb") as f:
        pickle.dump(model, f)
    print("Ok")
