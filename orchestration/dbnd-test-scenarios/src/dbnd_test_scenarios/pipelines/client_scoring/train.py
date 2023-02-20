# © Copyright Databand.ai, an IBM Company 2022

import datetime
import itertools
import logging
import pickle

from typing import List, Tuple

import click
import matplotlib
import numpy as np
import pandas as pd

from matplotlib import figure
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from dbnd import log_dataframe, log_metric, parameter, pipeline, task
from dbnd_test_scenarios.scenarios_repo import client_scoring_data
from dbnd_test_scenarios.utils.data_chaos_monkey.client_scoring_chaos import (
    chaos_model_metric,
)
from targets.types import PathStr


TARGET_LABEL = "target_label"
SELECTED_FEATURES = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", TARGET_LABEL]
logger = logging.getLogger(__name__)
matplotlib.use("Agg")


# Utilities
@task
def calculate_metrics(actual, pred, target_date=None, additional_name=""):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)

    r2 = chaos_model_metric(r2, target_date)
    rmse = chaos_model_metric(rmse, target_date)

    log_metric("rmse{}".format(additional_name), rmse)
    log_metric("mae{}".format(additional_name), mae)
    log_metric("r2{}".format(additional_name), r2)
    return rmse, mae, r2


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


@task
def calculate_features(
    data: pd.DataFrame, selected_features: List[str] = None, data_path: PathStr = None
) -> pd.DataFrame:
    log_dataframe("data_path", data, with_histograms=True, path=data_path)
    data = data[selected_features]
    return data


@task(result="training_set, test_set, validation_set")
def split_data(
    raw_data: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw_data.drop(["id", "1_norm", "10_norm"], axis=1, inplace=True, errors="ignore")
    log_dataframe("raw", raw_data, with_histograms=True)
    train_df, test_df = train_test_split(raw_data)
    test_df, validation_df = train_test_split(test_df, test_size=0.5)

    return train_df, test_df, validation_df


@task
def train_model(
    test_set: pd.DataFrame,
    training_set: pd.DataFrame,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    target_date: datetime.date = None,
) -> ElasticNet:
    print(training_set)
    print(training_set.shape)
    print("%s" % str({col: str(type_) for col, type_ in training_set.dtypes.items()}))
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
    lr.fit(training_set.drop([TARGET_LABEL], 1), training_set[[TARGET_LABEL]])
    prediction = lr.predict(test_set.drop([TARGET_LABEL], 1))

    (rmse, mae, r2) = calculate_metrics(
        test_set[[TARGET_LABEL]],
        prediction,
        target_date=target_date,
        additional_name="_train",
    )

    logging.info(
        "Elasticnet model (alpha=%f, l1_ratio=%f): rmse = %f, mae = %f, r2 = %f",
        alpha,
        l1_ratio,
        rmse,
        mae,
        r2,
    )
    return lr


@task(result=("report", "prediction_scatter_plot"))
def validate_model_for_customer(
    model: ElasticNet,
    validation_dataset: pd.DataFrame,
    threshold=0.2,
    target_date: datetime.date = None,
) -> Tuple[str, figure.Figure]:
    log_dataframe("validation", validation_dataset)

    # support for py3 parqeut
    validation_dataset = validation_dataset.rename(str, axis="columns")
    validation_x = validation_dataset.drop([TARGET_LABEL], 1)
    validation_y = validation_dataset[[TARGET_LABEL]]

    prediction = model.predict(validation_x)
    (rmse, mae, r2) = calculate_metrics(
        validation_y, prediction, target_date=target_date, additional_name="_validate"
    )

    fig = create_scatter_plot(validation_y, prediction)
    # if r2 < threshold:
    #     raise Exception(
    #         "Model quality is below threshold. Got R2 equal to %s, expect at least %s"
    #         % (r2, threshold)
    #     )

    return "%s,%s,%s" % (rmse, mae, r2), fig


@task
def train_partner_model(
    data: pd.DataFrame = None,
    data_path: PathStr = None,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    selected_features: List[str] = None,
    target_date: datetime.date = None,
):
    selected_features = selected_features or SELECTED_FEATURES
    data = calculate_features(
        selected_features=selected_features, data=data, data_path=data_path
    )
    training_set, test_set, validation_set = split_data(raw_data=data)

    model = train_model(
        test_set=test_set,
        training_set=training_set,
        alpha=alpha,
        l1_ratio=l1_ratio,
        target_date=target_date,
    )
    validation, _ = validate_model_for_customer(
        model=model, validation_dataset=validation_set, target_date=target_date
    )
    return model, validation


@pipeline
def train_for_all_customer(customer=parameter[List[pd.DataFrame]]):
    result = {}
    for c in customer:
        (model, validation) = train_partner_model(data=c)
        result[str(c)] = (model, validation)
    return result


@pipeline
def training_with_parameter_search(
    data: pd.DataFrame = client_scoring_data.p_g_train_data,
    alpha_step: float = 0.3,
    l1_ratio_step: float = 0.4,
):
    result = {}
    variants = list(
        itertools.product(np.arange(0, 1, alpha_step), np.arange(0, 1, l1_ratio_step))
    )
    logger.info("All Variants: %s", variants)
    for alpha_value, l1_ratio in variants:
        exp_name = "train_%f_%f" % (alpha_value, l1_ratio)
        model, validation = train_partner_model(
            data=data, alpha=alpha_value, l1_ratio=l1_ratio, task_name=exp_name
        )

        result[exp_name] = (model, validation)
    return result


@task
def run_train(train_data, output_model, target_date_str=None):
    if target_date_str:
        target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
    else:
        target_date = None
    data = pd.read_csv(train_data)
    model, validation = train_partner_model(
        data, data_path=train_data, target_date=target_date
    )
    with open(output_model, "wb") as f:
        pickle.dump(model, f)


@click.command()
@click.option(
    "--train-data", default=client_scoring_data.p_g_train_data, help="Training data"
)
@click.option(
    "--output-model", default="/tmp/trained_model.pickle", help="Output model location"
)
def cli_run_train_model(train_data, output_model):
    run_train(train_data=train_data, output_model=output_model)
    click.echo("Your model is ready at %s!" % output_model)


if __name__ == "__main__":
    cli_run_train_model()
