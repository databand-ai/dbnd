# This example requires pandas, numpy, sklearn, scipy
# Inspired by an MLFlow tutorial:
#   https://github.com/databricks/mlflow/blob/master/example/tutorial/train.py

import itertools
import logging

from typing import Any, Tuple

import matplotlib
import numpy as np
import pandas as pd

from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from dbnd import band, log_metric, output, task
from dbnd._core.commands import log_artifact
from dbnd_examples.data import data_repo


matplotlib.use("Agg")
logger = logging.getLogger(__name__)


# dbnd run -m dbnd_examples predict_wine_quality --task-version now
# dbnd run -m dbnd_examples predict_wine_quality_parameter_search --task-version now


def calculate_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2


@task(result=("training_set", "test_set", "validation_set"))
def prepare_data(raw_data):
    # type: (pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
    """ Split data into train, test and validation (py27) """
    train_df, test_df = train_test_split(raw_data)
    test_df, validation_df = train_test_split(test_df, test_size=0.5)

    return {
        "training_set": train_df,
        "test_set": test_df,
        "validation_set": validation_df,
    }


@task
def calculate_alpha(alpha=0.5):
    # type: (float)-> float
    """ Calculates alpha for train_model (py27) """
    alpha += 0.1
    return alpha


@task
def train_model(test_set, training_set, alpha=0.5, l1_ratio=0.5):
    # type: (pd.DataFrame, pd.DataFrame, float, float) -> ElasticNet
    """ Train wine prediction model (py27) """
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio)
    lr.fit(training_set.drop(["quality"], 1), training_set[["quality"]])
    prediction = lr.predict(test_set.drop(["quality"], 1))

    (rmse, mae, r2) = calculate_metrics(test_set[["quality"]], prediction)

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


def _create_scatter_plot(actual, predicted):
    import matplotlib.pyplot as plt

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax.set_title("Actual vs. Predicted")
    ax.set_xlabel("Actual Labels")
    ax.set_ylabel("Predicted Values")
    ax.scatter(actual, predicted)
    return fig


@task(result=output.txt())
def validate_model(model, validation_dataset):
    # type: (ElasticNet, pd.DataFrame) -> str
    """ Calculates metrics of wine prediction model (py27) """
    validation_x = validation_dataset.drop(["quality"], 1)
    validation_y = validation_dataset[["quality"]]

    prediction = model.predict(validation_x)
    (rmse, mae, r2) = calculate_metrics(validation_y, prediction)

    log_artifact(
        "prediction_scatter_plot", _create_scatter_plot(validation_y, prediction)
    )

    log_metric("rmse", rmse)
    log_metric("mae", rmse)
    log_metric("r2", r2)

    return "%s,%s,%s" % (rmse, mae, r2)


@band(result=("model", "validation"))
def predict_wine_quality(
    data=data_repo.wines, alpha=0.5, l1_ratio=0.5, good_alpha=False
):
    # type: (pd.DataFrame, float, float, bool) -> Any
    """ Entry point for wine quality prediction (py27) """
    training_set, test_set, validation_set = prepare_data(raw_data=data)

    if good_alpha:
        alpha = calculate_alpha(alpha)

    model = train_model(
        test_set=test_set, training_set=training_set, alpha=alpha, l1_ratio=l1_ratio
    )

    validation = validate_model(model=model, validation_dataset=validation_set)

    return model, validation


@band(result=("serving", "test"))
def predict_wine_quality_push_to_production(endpoint_name="wineapp"):
    from dbnd_examples.pipelines.wine_quality.serving.sagemaker import (
        deploy_to_sagemaker,
        push_image_to_ecr,
    )
    from dbnd_examples.pipelines.wine_quality.serving import docker, sagemaker

    model, validation = predict_wine_quality()

    package = docker.package_as_docker(model=model)
    push = push_image_to_ecr(image=package, name=endpoint_name)

    serving = deploy_to_sagemaker(
        app_name=endpoint_name, image_url=push, model_path=model
    )

    test = sagemaker.test_sagemaker_endpoint.t(endpoint_name=endpoint_name)
    test.set_upstream(serving)

    return serving, test


@band
def predict_wine_quality_parameter_search(
    data=data_repo.wines, alpha_step=0.3, l1_ratio_step=0.4
):
    # type: (pd.DataFrame, float, float) -> Any
    result = {}
    variants = list(
        itertools.product(np.arange(0, 1, alpha_step), np.arange(0, 1, l1_ratio_step))
    )
    logger.info("All Variants: %s", variants)
    for alpha_value, l1_ratio in variants:
        exp_name = "Predict_%f_l1_ratio_%f" % (alpha_value, l1_ratio)
        model, validation = predict_wine_quality(
            data=data, alpha=alpha_value, l1_ratio=l1_ratio, task_name=exp_name
        )
        result[exp_name] = (model, validation)

    return result
