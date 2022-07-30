# © Copyright Databand.ai, an IBM Company 2022

import logging

from datetime import datetime, timedelta
from typing import Tuple

import airflow

from airflow import DAG
from airflow.models import TaskInstance
from airflow.utils.dates import days_ago

from dbnd import task
from dbnd_airflow.constants import AIRFLOW_VERSION_2


if AIRFLOW_VERSION_2:
    from airflow.operators.python import PythonOperator
else:
    from airflow.operators.python_operator import PythonOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "dbnd_config": {"my_task.p_int_with_default": 4},
}

# support airflow 1.10.0
if airflow.version.version == "1.10.0":

    class PythonOperator_airflow_1_10_0(PythonOperator):
        template_fields = ("templates_dict", "op_kwargs")

    PythonOperator = PythonOperator_airflow_1_10_0


@task
def my_task(p_int=3, p_str="check", p_int_with_default=0):
    # type: (int, str, int) -> str
    logging.info("I am running")
    return "success"


@task
def my_multiple_outputs(p_str="some_string"):
    # type: (str) -> Tuple[int, str]
    return 1, p_str + "_extra_postfix"


def some_python_function(input_path, output_path):
    logging.error("I am running")
    input_value = open(input_path, "r").read()
    with open(output_path, "w") as fp:
        fp.write(input_value)
        fp.write("\n\n")
        fp.write(str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S")))
    return "success"


with DAG(dag_id="dbnd_operators", default_args=default_args) as dag_operators:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = my_task(2)
    t2, t3 = my_multiple_outputs(t1)
    tp = PythonOperator(
        task_id="some_python_function",
        python_callable=some_python_function,
        op_kwargs={"input_path": t3, "output_path": "/tmp/output.txt"},
    )
    tp.set_upstream(t3.op)

    t1_op = t1.op


if __name__ == "__main__":
    ti = TaskInstance(t1_op, days_ago(0))
    ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True)
    # #
    #
    # dag_operators.clear()
    # dag_operators.run()
