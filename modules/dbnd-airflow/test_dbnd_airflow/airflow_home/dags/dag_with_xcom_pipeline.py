# © Copyright Databand.ai, an IBM Company 2022

from datetime import datetime, timedelta
from typing import Tuple

from airflow.utils.dates import days_ago

from dbnd import pipeline, task


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@task
def bool_to_string(p_bool):
    # type: (bool) -> str
    return str(p_bool)


@task
def date_correction(p_date=None):
    # type: (datetime) -> datetime
    if p_date.day % 2 == 0:
        return p_date
    else:
        return p_date.replace(day=12)


@task
def convert_to_string(p_date=None):
    # type: (datetime) -> str
    return p_date.isoformat()


@task
def parse_to_string(p_date=None):
    # type: (datetime ) -> str
    split = p_date.isoformat().split("T")
    return "SEPARATOR".join(split)


@pipeline
def my_xcom_pipeline(p_date=None):
    # type: ( datetime ) -> Tuple[str, str]
    corrected_date = date_correction(p_date=p_date)
    string_date = convert_to_string(p_date=corrected_date)
    parsed_string = my_second_pipeline(p_date=corrected_date)
    return parsed_string, string_date


@pipeline
def my_second_pipeline(p_date=None):
    # type: ( datetime ) -> str
    parsed_string = parse_to_string(p_date)
    return parsed_string


# with DAG(dag_id="my_xcom_dag", default_args=default_args) as dag:
#     my_xcom_pipeline(p_date="{{ ts }}")
