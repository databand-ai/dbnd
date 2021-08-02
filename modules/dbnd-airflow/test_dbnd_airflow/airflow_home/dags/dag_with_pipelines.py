import logging

from datetime import timedelta
from typing import Tuple

from airflow import DAG
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
def my_task(p_int=3, p_str="check", p_int_with_default=0):
    # type: (int, str, int) -> str
    logging.info("I am running")
    return "success"


@task
def my_multiple_outputs(p_str="some_string"):
    # type: (str) -> Tuple[int, str]
    return 1, p_str + "_extra_postfix"


@pipeline
def my_pipeline(p_str="some_string"):
    # type: (str) -> Tuple[int, str]
    # native_input_op = BashOperator(
    #     task_id="native_input_op", bash_command="echo hello_airflow", xcom_push=True
    # )

    t1 = my_task(p_int=2, p_str="native_input_op")
    t2, t3 = my_multiple_outputs(t1)

    # airflow operator
    # native_output_op = BashOperator(
    #     task_id="native_output_op", bash_command="echo %s" % t2, retries=3
    # )
    # t2.set_downstream(native_output_op)
    return t2, t3


@pipeline
def my_pipeline_search(x_range=3):
    results = {}
    for x in range(x_range):
        t2, t3 = my_pipeline(p_str=str(x))
        results[x] = t3
    return results


with DAG(dag_id="dbnd_pipeline", default_args=default_args) as dag_dbnd_pipeline:
    my_pipeline()

with DAG(dag_id="dbnd_search", default_args=default_args) as dag_dbnd_search:
    my_pipeline_search(2)
