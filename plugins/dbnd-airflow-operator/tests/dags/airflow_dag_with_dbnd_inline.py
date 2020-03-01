"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
import logging

from datetime import datetime, timedelta
from typing import Tuple

import airflow

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dbnd import dbnd_config, pipeline, task


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dbnd_config.load_system_configs()
dbnd_config.set("log", "disabled", True)


# dbnd_config.set("databand", "env", "aws")


@task
def my_task(p_int=3, p_str="check") -> str:
    logging.error("I am running")
    return "success"


@task
def my_multiple_outputs(p_str="some_string") -> Tuple[int, str]:
    return 1, p_str + "_extra_postfix"


def some_python_function(input_path, output_path):
    logging.error("I am running")
    input_value = open(input_path, "r").read()
    with open(output_path, "w") as fp:
        fp.write(input_value)
        fp.write("\n\n")
        fp.write(str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S")))
    return "success"


#
with DAG(dag_id="dbnd_operators", default_args=default_args) as dag_operators:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # t1 = my_task(2)
    t1 = my_task(2)
    t2, t3 = my_multiple_outputs(t1)
    tp = PythonOperator(
        task_id="some_python_function",
        python_callable=some_python_function,
        op_kwargs={"input_path": t3, "output_path": "/tmp/output.txt"},
    )
    tp.set_upstream(t3.op)


@pipeline
def my_pipeline(p_str="some_string") -> Tuple[int, str]:
    t1 = my_task(p_int=2, p_str=p_str)
    t2, t3 = my_multiple_outputs(t1)
    return t2, t3


@pipeline
def my_pipeline_search(x_range=3):
    results = {}
    for x in range(x_range):
        t2, t3 = my_pipeline(p_str=str(x))
        results[x] = t3
    return results


with DAG(dag_id="dbnd_search", default_args=default_args) as dag_dbnd_operators:
    my_pipeline_search(2)
# print(t2)
#
# with DAG("predict_wine", default_args=default_args) as dag_predict:
#     predict_wine_quality(data="dbnd-core/examples/data/wine_quality.csv")
#
#     a = someTask()
#     b = BashOperator(command= a )
# with DAG("dbnd_pipeline", default_args=default_args) as dag_pipeline:
#     t1 = my_pipeline("some_value")
# print(dag_pipeline.tasks)
