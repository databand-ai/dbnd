import datetime
import logging
import shutil
import sys
from random import randint, random
from typing import List, Tuple

import pandas as pd
from sklearn import preprocessing

from dbnd import log_dataframe, log_metric, pipeline, task
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.utils.basics.exportable_strings import get_hashed_name
from dbnd._core.utils.basics.range import period_dates
from dbnd_test_scenarios.pipelines.common.pandas_tasks import load_from_sql_data
from dbnd_test_scenarios.scenarios_repo import client_scoring_data
from targets import target

logger = logging.getLogger(__name__)

@task
def enrich_missing_fields(
    raw_data=parameter(log_histograms=True)[pd.DataFrame],
    columns_to_impute=None,
    fill_with=0,
) -> pd.DataFrame:
    columns_to_impute = columns_to_impute or ["10"]
    counter = int(raw_data[columns_to_impute].copy().isna().sum())
    noise = randint(-counter, counter)
    log_metric(
        "Replaced NaNs", int(raw_data[columns_to_impute].copy().isna().sum()) + noise
    )
    raw_data[columns_to_impute] = raw_data[columns_to_impute].fillna(fill_with)
    return raw_data


@task
def enrich_with_scaler(raw_data: pd.DataFrame, column_name: str) -> pd.DataFrame:
    scaler = preprocessing.MinMaxScaler()
    raw_data[column_name + "_norm"] = scaler.fit_transform(
        raw_data[[column_name]].values.astype(float)
    )
    return raw_data


@task
def clean_pii(data: pd.DataFrame, pii_columns) -> pd.DataFrame:
    data[pii_columns] = data[pii_columns].apply(
        lambda x: x.apply(get_hashed_name), axis=1
    )
    log_metric("PII items removed:", len(pii_columns) * data.shape[0])
    log_dataframe("pii_clean", data)
    return data


@task
def dedup_records(data: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    data = data.drop_duplicates(subset=columns)
    item_count = len(columns) * data.shape[0]
    noise = randint(-item_count, item_count)
    log_metric("Removed Duplicates", len(columns) * data.shape[0] + noise)
    return data


@task
def create_report(data: pd.DataFrame) -> str:
    avg_score = int(
        data["score"].sum() + randint(-2 * len(data.columns), 2 * len(data.columns))
    )

    log_metric("Column Count", len(data.columns))
    log_metric("Avg Score", avg_score)
    log_dataframe("ready_data", data)
    if "client_score" in data.columns:
        desc = data["client_score"].describe()[["mean", "std"]].to_dict()
        log_metric(
            "Client Score Stats", "Mean: %.2f. Std: %.2f." % (desc["mean"], desc["std"])
        )

    return "0,10,12"


@pipeline
def ingest_partner_data(
    data=parameter(log_histograms=True)[pd.DataFrame],
    name="customer",
    dedup_columns=None,
    columns_to_impute=None,
    pii_columns=None,
) -> Tuple[str, pd.DataFrame]:
    pii_columns = pii_columns or ["name", "address", "phone"]
    dedup_columns = dedup_columns or ["phone"]
    columns_to_impute = columns_to_impute or ["10"]

    imputed = enrich_missing_fields(data, columns_to_impute)
    clean = clean_pii(imputed, pii_columns)
    deduped = dedup_records(clean, columns=dedup_columns)
    result = create_report(deduped)
    return result, deduped


# PARTNERS DATA
def partner_file_data_location(name, task_target_date):
    rand = random()
    if rand < 0.2:
        partner_file = "data/big_file.csv"
    else:
        partner_file = "data/small_file.csv"

    return target(partner_file)


def run_fetch_customer_data(partner_name, output_path):
    target(output_path).mkdir_parent()
    shutil.copy(client_scoring_data.p_a_master_data, output_path)
    return output_path


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
    report[1].to_csv(output_file)
    return output_file


def run_enrich_missing_fields(input_path, output_path):
    enrich_missing_fields(pd.read_csv(input_path)).to_csv(output_path)
    return output_path


def run_clean_piis(input_path, output_path):
    log_metric("input path", input_path)
    clean_pii(pd.read_csv(input_path), ["name", "address", "phone"]).to_csv(output_path)
    return output_path


def run_dedup_records(data_path, output_path, columns=None, **kwargs):
    dedup_records(data=pd.read_csv(data_path)).to_csv(output_path, columns=columns)
    return output_path


def run_func(func, input_path, output_path, **kwargs):
    logger.info("Calling %s with  %s -> %s (extra args: %s)", func, input_path, output_path, kwargs)
    func(pd.read_csv(input_path), **kwargs).to_csv(output_path)
    return output_path


if __name__ == "__main__":
    run_process_customer_data(sys.argv[1], sys.argv[2])
