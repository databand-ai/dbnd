import hashlib
import sys

from random import randint

import pandas as pd

from dbnd import log_dataframe, log_metric, parameter, task
from dbnd._core.utils.basics.exportable_strings import get_hashed_name


def get_hash(value: str) -> str:
    return hashlib.md5(value.encode()).hexdigest()


@task
def clean_pii(data: pd.DataFrame, pii_columns) -> pd.DataFrame:
    data[pii_columns] = data[pii_columns].apply(
        lambda x: x.apply(get_hashed_name), axis=1
    )
    log_metric("PII items removed:", len(pii_columns) * data.shape[0])
    return data.copy()


def clean_piis_file(input_path, output_path):
    """This is a function that will run within the DAG execution"""
    df = pd.read_csv(input_path)
    log_metric("input path", input_path)
    output = clean_pii(df, ["name", "address", "phone"])
    log_dataframe("output_df", output)
    output.to_csv(output_path)


@task(raw_data=parameter(log_histograms=True)[pd.DataFrame])
def unit_imputation(
    raw_data: pd.DataFrame, columns_to_impute=None, value=0
) -> pd.DataFrame:
    columns_to_impute = columns_to_impute or ["10"]
    counter = int(raw_data[columns_to_impute].copy().isna().sum())
    noise = randint(-counter, counter)
    log_metric(
        "Replaced NaNs", int(raw_data[columns_to_impute].copy().isna().sum()) + noise
    )

    raw_data[columns_to_impute] = raw_data[columns_to_impute].fillna(value)
    return raw_data.copy()


def unit_imputation_file(input_path, output_path):
    """This is a function that will run within the DAG execution"""
    df = pd.read_csv(input_path)
    log_metric("input path", input_path)
    output = unit_imputation(df, ["10"])
    log_dataframe("output_df", output)
    output.to_csv(output_path)
    return output_path


@task(data=parameter(log_histograms=True)[pd.DataFrame])
def dedup_records(data: pd.DataFrame, columns) -> pd.DataFrame:
    # data[columns] = data[columns].apply(lambda x: x.apply(get_hash), axis=1)
    data = data.drop_duplicates(subset=columns)
    item_count = len(columns) * data.shape[0]
    noise = randint(-item_count, item_count)
    log_metric("Removed Duplicates:", len(columns) * data.shape[0] + noise)
    return data


@task
def dedup_records_file(data_path, output_path, columns=None, **kwargs):
    if columns is None:
        columns = ["name"]

    data = dedup_records(data=pd.read_csv(data_path))
    data.to_csv(output_path)
    return output_path


@task
def create_report(data_path, output_path, **kwargs):
    data = pd.read_csv(data_path)
    log_dataframe("data", data, with_histograms=True)
    log_metric("Column Count", len(data.columns))
    log_metric(
        "avg score",
        int(
            data["score"].sum() + randint(-2 * len(data.columns), 2 * len(data.columns))
        ),
    )
    data.to_csv(output_path)
    return output_path


@task(data=parameter(log_histograms=True)[pd.DataFrame])
def create_report(data: pd.DataFrame) -> pd.DataFrame:
    log_metric("Column Count", len(data.columns))
    log_metric(
        "Avg Score",
        int(
            data["score"].sum() + randint(-2 * len(data.columns), 2 * len(data.columns))
        ),
    )
    return data.copy()


@task(data=parameter(log_histograms=True)[pd.DataFrame])
def process_customer_data(
    data: pd.DataFrame, dedup_columns=None, columns_to_impute=None, value=0
) -> pd.DataFrame:
    columns_to_impute = columns_to_impute or ["10"]
    dedup_columns = dedup_columns or ["name"]
    imputed = unit_imputation(data, columns_to_impute, value)
    clean = dedup_records(imputed, dedup_columns)
    result = create_report(clean)
    log_dataframe("customer_data", result, path="/airflow/customer_data.csv")
    return result


@task
def load_from_sql_data(sql_conn_str, query) -> pd.DataFrame:
    import sqlalchemy

    engine = sqlalchemy.create_engine(sql_conn_str)
    return pd.read_sql(query, engine)


def process_customer_data_from_sql(query, sql_conn_str, output_file):
    data = load_from_sql_data(sql_conn_str=sql_conn_str, query=query)
    report = process_customer_data(data)
    report.to_csv(output_file)
    return output_file


def process_customer_data_from_file(input_file, output_file):
    report = process_customer_data(pd.read_csv(input_file))
    report.to_csv(output_file)
    return output_file


if __name__ == "__main__":
    process_customer_data_from_file(sys.argv[1], sys.argv[2])
