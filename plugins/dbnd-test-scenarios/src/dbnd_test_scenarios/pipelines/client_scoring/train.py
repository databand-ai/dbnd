import itertools
import logging
import pickle

from typing import List, Tuple

import matplotlib
import numpy as np
import pandas as pd

from matplotlib import figure
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

import click

from dbnd import log_dataframe, log_metric, parameter, pipeline, task
from dbnd_test_scenarios.scenarios_repo import client_scoring_data


logger = logging.getLogger(__name__)
matplotlib.use("Agg")


# Utilities
@task
def calculate_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)

    log_metric("rmse", rmse)
    log_metric("mae", mae)
    log_metric("r2", r2)
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
    data: pd.DataFrame, selected_features: List[str] = None
) -> pd.DataFrame:
    if selected_features:
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
) -> ElasticNet:
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
    lr.fit(training_set.drop(["target"], 1), training_set[["target"]])
    prediction = lr.predict(test_set.drop(["target"], 1))

    (rmse, mae, r2) = calculate_metrics(test_set[["target"]], prediction)

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
def train_partner_model(
    data: pd.DataFrame = None,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    selected_features: List[str] = None,
):
    data = calculate_features(selected_features=selected_features, data=data)
    training_set, test_set, validation_set = split_data(raw_data=data)

    model = train_model(
        test_set=test_set, training_set=training_set, alpha=alpha, l1_ratio=l1_ratio
    )
    validation, _ = validate_model_for_customer(
        model=model, validation_dataset=validation_set
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
    data: pd.DataFrame = client_scoring_data.p_a_master_data,
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


@click.command()
@click.option(
    "--train-data", default=client_scoring_data.train_data, help="Training data"
)
@click.option(
    "--output-model", default="/tmp/trained_model.pickle", help="Output model location"
)
def cli_run_train_model(train_data, output_model):
    model, validation = train_partner_model(pd.read_csv(train_data))
    with open(output_model, "wb") as f:
        pickle.dump(model, f)
    click.echo("Your model is ready at %s!" % output_model)


if __name__ == "__main__":
    cli_run_train_model()
