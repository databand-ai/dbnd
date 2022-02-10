from datetime import timedelta

import airflow

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dbnd import dont_track
from dbnd_airflow import track_dag


class TestDocTrackingAirflowDags:
    def test_prepare_data_operator(self):
        #### DOC START
        from airflow.models import BaseOperator

        from dbnd import log_metric, task

        class PrepareData(BaseOperator):
            def execute(self, context):
                calculate_alpha()
                prepare_data()

        def calculate_alpha():
            log_metric("alpha", 0.5)

        @task
        def prepare_data():
            log_metric("lines", 10)
            log_metric("chars", 300)

        #### DOC END
        test_preparedata = PrepareData(task_id="test_PrepareData").execute("<CONTEXT>")

    def test_dont_track(self):
        #### DOC START
        @dont_track
        def calculate_beta():
            return 1.5

        #### DOC END
        calculate_beta()

    def test_track_specific_dags(self):
        #### DOC START
        args = {"start_date": airflow.utils.dates.days_ago(2)}
        dag = DAG(
            dag_id="calculate_alpha",
            default_args=args,
            schedule_interval=timedelta(days=1),
        )

        def calculate_alpha():
            return 0.5

        PythonOperator(
            task_id="calculate_alpha", python_callable=calculate_alpha, dag=dag
        )

        track_dag(dag)
        #### DOC END
