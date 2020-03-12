import logging

from datetime import timedelta
from typing import Tuple

from airflow.utils.dates import days_ago

from dbnd import pipeline, task


default_args_test = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


@task
def t_A(p_str="check", p_int=2):
    # type: (str,int) -> str
    logging.info("I am running")
    return p_str * p_int


@task
def t_B(pb_str="check", expected=None):
    # type: (str,str)  -> str
    if expected:
        assert pb_str == expected
    logging.info("I am running")
    return pb_str + "t_B"


@task
def t_two_outputs(pb_str="check"):
    # type: (str) -> Tuple[str, int]
    logging.info("t_two_outputs is running")
    return pb_str + "t_B", len(pb_str)


@task
def t_with_fields(p_str="from_ctor", p2_str="from_ctor"):
    # type: (str,str) -> str
    return p_str + p2_str


@pipeline
def t_pipeline(p_str="from_pipeline_ctor"):
    # type: (str) -> str
    a = t_A(p_str)
    return t_B(a)


@pipeline
def t_pipeline_of_pipeline(p_str="from_pipeline_pipeline_ctor"):
    # type: (str) -> Tuple[str, str]
    a = t_pipeline(p_str)
    b = t_pipeline(a)
    return a, b
