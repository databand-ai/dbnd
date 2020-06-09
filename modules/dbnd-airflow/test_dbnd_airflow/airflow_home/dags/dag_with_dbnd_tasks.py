import logging

from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from dbnd import Task, parameter


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "dbnd_config": {"my_task.p_int_with_default": 4},
}


class MyTask(Task):
    p_int = parameter.value(3)
    p_str = parameter.value("check")
    p_int_with_default = parameter.value(0)

    output_str = parameter.output[str]

    def run(self):
        logging.info("I am running")
        self.output_str = "success"


class MyMultipleOutputs(Task):
    p_str = parameter.value("some_string")
    p_int_with_default = parameter.value(0)

    output_str = parameter.output[str]
    output_int = parameter.output[int]

    def run(self):
        logging.info("I am running")
        self.output_str = "success"
        self.output_int = 2


with DAG(dag_id="dbnd_tasks", default_args=default_args) as dag_tasks:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = MyTask(p_int=2)
    t2 = MyMultipleOutputs(p_str=t1.output_str)

    t1_op = t1.op
    t2_op = t2.op

if __name__ == "__main__":
    # ti = TaskInstance(t1_op, days_ago(0))
    # ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True)
    # #
    #
    dag_tasks.clear()
    dag_tasks.run(days_ago(0), days_ago(0))
