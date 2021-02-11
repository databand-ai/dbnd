import os

from warnings import warn

import airflow

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from numpy import issubdtype, mean, median, number
from pandas import DataFrame

from dbnd import log_dataframe, log_metric
from dbnd._core.constants import DbndTargetOperationType


REDSHIFT_CONNECTION_ID = "redshift_conn"
REDSHIFT_TABLE = "transaction_data"
VIEW_LIMIT = 100

# queries
SELECT_DATA = f"SELECT * FROM {REDSHIFT_TABLE} LIMIT %s;"

DEFAULT_ARGS = {
    "owner": "databand",
    "start_date": airflow.utils.dates.days_ago(2),
    "provide_context": False,
}


def monitor_redshift_table(**op_kwarg):
    hook = PostgresHook(REDSHIFT_CONNECTION_ID)
    data = hook.get_pandas_df(SELECT_DATA, parameters=[VIEW_LIMIT])

    log_dataframe(
        f"{REDSHIFT_TABLE}",
        data,
        with_histograms=True,
        with_stats=True,
        with_schema=True,
    )

    # log_metric("record count", data.shape[0])
    log_metric("Duplicate records", data.duplicated().sum())
    for column in data.columns:
        log_metric(f"{column} null record count", int(data[column].isna().sum()))

        if issubdtype(data[column].dtype, number):
            log_metric(f"{column} mean", data[column].mean())
            log_metric(f"{column} median", data[column].median())
            log_metric(f"{column} min", data[column].min())
            log_metric(f"{column} max", data[column].max())
            log_metric(f"{column} std", data[column].std())


with DAG(
    dag_id=f"dbnd_redshift_{REDSHIFT_TABLE}_monitor",
    schedule_interval="@daily",  # every 12 hours
    default_args=DEFAULT_ARGS,
    tags=["python", "redshift", "monitor"],
) as dbnd_template_dag:

    redshift_monitor = PythonOperator(
        task_id="monitor_redshift_table", python_callable=monitor_redshift_table
    )
