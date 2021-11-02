import datetime
import itertools
import logging

from typing import List, Tuple

import matplotlib
import numpy as np
import pandas as pd

from matplotlib import figure
from sklearn import preprocessing
from sklearn.linear_model import ElasticNet
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from dbnd import log_dataframe, log_metric, parameter, pipeline, task
from dbnd.utils import data_combine, period_dates
from dbnd_examples.data import demo_data_repo
from dbnd_examples.orchestration.examples.wine_quality import package_as_docker
from targets import target


logger = logging.getLogger(__name__)
matplotlib.use("Agg")


def data_source(task_target_date, name):
    return target(demo_data_repo.partner_to_file(name, task_target_date))


@task
def get_and_enrich(raw_data: pd.DataFrame, column_name: str) -> pd.DataFrame:
    scaler = preprocessing.MinMaxScaler()
    raw_data[column_name + "_norm"] = scaler.fit_transform(
        raw_data[[column_name]].values.astype(float)
    )
    return raw_data


@task
def clean_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    return raw_data.fillna(0)


@pipeline
def ingest_partner_a(task_target_date):
    raw_data = data_source(name="a", task_target_date=task_target_date)
    clean = clean_data(raw_data=raw_data)
    return get_and_enrich(raw_data=clean, column_name="0")


@pipeline
def ingest_partner_b(task_target_date):
    raw_data = data_source(name="b", task_target_date=task_target_date)
    data = clean_data(raw_data=raw_data)
    return data


@pipeline
def ingest_partner_c(task_target_date):
    raw_data = data_source(name="c", task_target_date=task_target_date)
    clean = clean_data(raw_data=raw_data)
    data = get_and_enrich(raw_data=clean, column_name="10")
    return data


@pipeline
def fetch_partner_data(
    task_target_date, selected_partners: List[str], period=datetime.timedelta(days=7)
) -> List[pd.DataFrame]:
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
    columns_to_remove = set(["id", "0_norm", "10_norm"])
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


@pipeline(result=("model", "validation"))
def train_model_for_customer(
    task_target_date,
    data: pd.DataFrame = None,
    alpha: float = 1.0,
    l1_ratio: float = 0.5,
    period=datetime.timedelta(days=7),
    selected_features: List[str] = None,
):
    if data is None:
        partners = fetch_partner_data(task_target_date=task_target_date, period=period)
        data = calculate_features(
            selected_features=selected_features, raw_data=partners
        )
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
        (model, validation) = train_model_for_customer(data=c)
        result[str(c)] = (model, validation)
    return result


@pipeline
def training_with_parameter_search(
    data: pd.DataFrame = demo_data_repo.sample_customer_file,
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


@pipeline(result=("model", "validation", "serving"))
def tran_model_and_package():
    model, validation = train_model_for_customer()
    serving = package_as_docker(model=model)
    return model, validation, serving


# Uttilities
def calculate_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2
