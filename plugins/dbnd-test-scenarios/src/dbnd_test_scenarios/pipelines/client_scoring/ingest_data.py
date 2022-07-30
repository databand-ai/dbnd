# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging
import shutil
import sys

from random import randint, random
from typing import List, Tuple

import pandas as pd

from sklearn import preprocessing

from dbnd import log_dataframe, log_metric, pipeline, task
from dbnd._core.constants import DbndTargetOperationType
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.utils.basics.range import period_dates
from dbnd_test_scenarios.pipelines.common.pandas_tasks import load_from_sql_data
from dbnd_test_scenarios.scenarios_repo import client_scoring_data
from dbnd_test_scenarios.utils.data_utils import get_hash_for_obj
from targets import target


logger = logging.getLogger(__name__)


def run_get_customer_data(partner_name, output_path, target_date_str):
    target(output_path).mkdir_parent()
    source_file = client_scoring_data.get_ingest_data(partner_name, target_date_str)
    shutil.copy(source_file, output_path)
    return output_path


@task
def clean_pii(
    data: pd.DataFrame, pii_columns: List[str], target_date: datetime.date = None
) -> pd.DataFrame:
    # I am not sure about this code, but this might help
    if target_date and target_date >= datetime.date(2020, 7, 12):
        if "10" not in data.columns:
            log_metric("Fixed columns", ["10"])
            data["10"] = 0
    data[pii_columns] = data[pii_columns].apply(
        lambda x: x.apply(get_hash_for_obj), axis=1
    )
    log_metric("PII items removed:", len(pii_columns) * data.shape[0])
    log_dataframe("pii_clean", data)
    return data


@task
def enrich_missing_fields(
    raw_data=parameter(log_histograms=True)[pd.DataFrame],
    columns_to_impute=None,
    columns_min_max_scaler=None,
    fill_with=0,
) -> pd.DataFrame:
    columns_to_impute = columns_to_impute or ["10"]
    columns_min_max_scaler = columns_min_max_scaler or []

    counter = int(raw_data[columns_to_impute].copy().isna().sum())
    noise = randint(-counter, counter)
    log_metric(
        "Replaced NaNs", int(raw_data[columns_to_impute].copy().isna().sum()) + noise
    )
    raw_data[columns_to_impute] = raw_data[columns_to_impute].fillna(fill_with)

    for column_name in columns_min_max_scaler:
        scaler = preprocessing.MinMaxScaler()
        raw_data[column_name + "_norm"] = scaler.fit_transform(
            raw_data[[column_name]].values.astype(float)
        )
    return raw_data


@task
def dedup_records(data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    data = data.drop_duplicates(subset=columns)
    item_count = len(columns) * data.shape[0]
    noise = randint(-item_count, item_count)
    log_metric("Removed Duplicates", len(columns) * data.shape[0] + noise)
    return data


@task
def create_report(data: pd.DataFrame) -> pd.DataFrame:
    avg_score = int(
        data["score_label"].sum()
        + randint(-2 * len(data.columns), 2 * len(data.columns))
    )
    log_metric("Column Count", len(data.columns))
    log_metric("Avg Score", avg_score)
    log_dataframe("ready_data", data, with_histograms=True)
    return pd.DataFrame(data=[[avg_score]], columns=["avg_score"])


@pipeline
def ingest_partner_data(
    data=parameter(log_histograms=True)[pd.DataFrame],
    name="customer",
    dedup_columns=None,
    columns_to_impute=None,
    pii_columns=None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    pii_columns = pii_columns or ["name", "address", "phone"]
    dedup_columns = dedup_columns or ["phone"]
    columns_to_impute = columns_to_impute or ["10"]

    clean = clean_pii(data, pii_columns)

    enriched = enrich_missing_fields(clean, columns_to_impute)
    deduped = dedup_records(enriched, columns=dedup_columns)
    report = create_report(deduped)
    return report, deduped


# PARTNERS DATA
def partner_file_data_location(name, task_target_date):
    rand = random()
    if rand < 0.2:
        partner_file = "data/big_file.csv"
    else:
        partner_file = "data/small_file.csv"

    return target(partner_file)


@pipeline
def fetch_partners_data(
    task_target_date, selected_partners: List[str], period=datetime.timedelta(days=7)
) -> List[pd.DataFrame]:
    all_data = []
    for partner in selected_partners:
        for d in period_dates(task_target_date, period):
            partner_data_file = partner_file_data_location(
                name=partner, task_target_date=d
            )
            partner_report, partner_data = ingest_partner_data(
                name=partner, data=partner_data_file
            )
            all_data.append(partner_data)
    return all_data


# ###########
# RUN FUNCTIONS,  (not sure if we need them)
def run_process_customer_data_from_sql(query, sql_conn_str, output_file):
    data = load_from_sql_data(sql_conn_str=sql_conn_str, query=query)
    report = ingest_partner_data(data)
    report[1].to_csv(output_file)
    return output_file


def run_process_customer_data(input_file, output_file):
    report = ingest_partner_data(pd.read_csv(input_file))
    report[1].to_csv(output_file, index=False)
    return output_file


def run_enrich_missing_fields(input_path, output_path, columns_to_impute=None):
    enrich_missing_fields(
        raw_data=pd.read_csv(input_path), columns_to_impute=columns_to_impute
    ).to_csv(output_path, index=False)
    return output_path


def run_clean_piis(input_path, output_path, pii_columns, target_date_str=None):
    target_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
    data = pd.read_csv(input_path)
    log_dataframe(
        "data",
        data,
        path=input_path,
        with_histograms=True,
        operation_type=DbndTargetOperationType.read,
    )
    clean_pii(data=data, pii_columns=pii_columns, target_date=target_date).to_csv(
        output_path, index=False
    )
    return output_path


def run_dedup_records(input_path, output_path, columns=None):
    dedup_records(data=pd.read_csv(input_path), columns=columns).to_csv(
        output_path, index=False
    )
    return output_path


def run_create_report(input_path, output_path):
    data = pd.read_csv(input_path)
    log_dataframe(
        "data",
        data,
        path=input_path,
        with_histograms=True,
        operation_type=DbndTargetOperationType.write,
    )
    create_report(data).to_csv(output_path, index=False)
    return output_path


if __name__ == "__main__":
    run_process_customer_data(sys.argv[1], sys.argv[2])
