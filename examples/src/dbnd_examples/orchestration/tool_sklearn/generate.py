# © Copyright Databand.ai, an IBM Company 2022

import datetime

import numpy as np
import pandas as pd

from dbnd import output, pipeline, task
from dbnd.utils import period_dates
from dbnd_examples.data import demo_data_repo, partner_data_file


@pipeline
def generate_partner_data(
    seed: pd.DataFrame = demo_data_repo.seed,
    task_target_date=datetime.datetime.now().date(),
    period=datetime.timedelta(days=7),
    bad_labels_date=datetime.datetime.strptime("2018-01-01", "%Y-%m-%d").date(),
):
    data = rename_columns(data=seed)
    data = calculate_target_variable(data=data)

    results = {}

    for d in period_dates(task_target_date, period):
        r = generate_for_date(
            task_target_date=d,
            data=data,
            a_out=demo_data_repo.partner_a_file(d),
            b_out=demo_data_repo.partner_b_file(d),
            c_out=demo_data_repo.partner_c_file(d),
        )
        results[str(d)] = r

    noisy_data = generate_for_date(
        task_target_date=bad_labels_date,
        data=data,
        noise=True,
        a_out=demo_data_repo.partner_a_file(bad_labels_date),
        b_out=demo_data_repo.partner_b_file(bad_labels_date),
        c_out=demo_data_repo.partner_c_file(bad_labels_date),
    )
    results[str(bad_labels_date)] = noisy_data

    customers = create_customer_files(
        seed=data,
        a_out=partner_data_file("customer_a.csv"),
        b_out=partner_data_file("customer_b.csv"),
    )
    results["customers"] = customers
    return results


@task
def create_customer_files(
    seed: pd.DataFrame, a_out=output.csv[pd.DataFrame], b_out=output.csv[pd.DataFrame]
):
    data = seed.copy(deep=True)
    data.to_target(a_out, index=False)
    data["target"] = np.random.randint(
        low=data.shape[0], high=100 * data.shape[0], size=data.shape[0]
    )
    data.to_target(b_out, index=False)
    return "Ok"


@task
def rename_columns(data: pd.DataFrame) -> pd.DataFrame:
    data.columns = list("0123456789") + ["10", "target"]
    return data


@task
def calculate_target_variable(data: pd.DataFrame) -> pd.DataFrame:
    data = data.drop(["target"], axis=1)
    ids = np.arange(data.shape[1])
    coefficients = (-1) ** ids * np.exp(-ids / 10)
    y = np.dot(data.values, coefficients)
    y += 0.01 * np.random.normal(size=data.shape[0])
    data["target"] = y

    return data


@task
def generate_for_date(
    task_target_date,
    data: pd.DataFrame,
    a_out=output.csv[pd.DataFrame],
    b_out=output.json[pd.DataFrame],
    c_out=output.csv[pd.DataFrame],
    noise=False,
):
    sample = data.sample(frac=0.2, random_state=task_target_date.day)
    partner_a = sample[list("012345")].copy()
    partner_b = sample[list("6789")].copy()
    partner_c = sample[["10", "target"]].copy()
    if noise:
        partner_c["target"] = np.random.randint(
            low=partner_c.shape[0],
            high=100 * partner_c.shape[0],
            size=partner_c.shape[0],
        )

    partner_a.index.names = ["id"]
    partner_b.index.names = ["id"]
    partner_b.reset_index(inplace=True)
    partner_c.index.names = ["id"]

    partner_a.to_target(a_out)
    partner_b.to_target(b_out)
    partner_c.to_target(c_out)

    return "Ok"


if __name__ == "__main__":
    generate_partner_data.dbnd_run(seed=demo_data_repo.seed, task_version="now")
