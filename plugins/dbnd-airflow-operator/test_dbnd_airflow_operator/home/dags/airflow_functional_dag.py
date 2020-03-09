import logging

from datetime import datetime, timedelta
from typing import Tuple

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from dbnd import task


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'dbnd_config': {
    #      "my_task.p_int": 4
    # }
}


@task
def my_task(p_int=3, p_str="check") -> str:
    logging.info("I am running")
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
