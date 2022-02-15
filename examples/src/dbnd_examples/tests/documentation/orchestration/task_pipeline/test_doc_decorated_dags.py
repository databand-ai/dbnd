import logging

from datetime import datetime, timedelta
from typing import Tuple

import airflow

from airflow import DAG
from airflow.utils.dates import days_ago

from dbnd import parameter, pipeline, task


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


class TestDocDecoratedDags:
    def test_code_example(self):
        #### DOC START
        # support airflow 1.10.0
        from airflow.operators.python_operator import PythonOperator

        if airflow.version.version == "1.10.0":

            class PythonOperator_airflow_1_10_0(PythonOperator):
                template_fields = ("templates_dict", "op_kwargs")

            PythonOperator = PythonOperator_airflow_1_10_0

        @task
        def calculate_alpha(alpha: int):
            logging.info("I am running")
            return alpha

        @task
        def prepare_data(data: str) -> Tuple[str, str]:
            return data, data

        def read_and_write(input_path, output_path):
            logging.error("I am running")
            input_value = open(input_path, "r").read()
            with open(output_path, "w") as fp:
                fp.write(input_value)
                fp.write("\n\n")
                fp.write(str(datetime.now().strftime("%Y-%m-%dT%H:%M:%S")))
            return "success"

        with DAG(dag_id="dbnd_operators", default_args=default_args) as dag_operators:
            # t1, t2 and t3 are examples of tasks created by instantiating operators
            t1 = calculate_alpha(2)
            t2, t3 = prepare_data(t1)
            tp = PythonOperator(
                task_id="some_python_function",
                python_callable=read_and_write,
                op_kwargs={"input_path": t3, "output_path": "/tmp/output.txt"},
            )
            tp.set_upstream(t3.op)

            t1_op = t1.op

            airflow_op_kwargs = {"priority_weight": 50}
            # Define DAG context
            with DAG(
                dag_id="dbnd_operators", default_args=default_args
            ) as dag_operators:
                t1 = calculate_alpha(2, task_airflow_op_kwargs=airflow_op_kwargs)
        #### DOC END
        assert t1_op

    def test_prepare_data_jinja_templating(self):
        #### DOC START
        @pipeline
        def current_date(p_date=None):
            return p_date

        with DAG(dag_id=f"current_date_dag", default_args=default_args) as dag:
            current_date(p_date="{{ ts }}")
        #### DOC END

    def test_prepare_data_no_jinja_templating(self):
        #### DOC START
        @pipeline
        def current_date(p_date=parameter[str].disable_jinja_templating):
            return p_date

        with DAG(dag_id=f"current_date_dag", default_args=default_args) as dag:
            current_date(p_date="{{ ts }}")
        #### DOC END
