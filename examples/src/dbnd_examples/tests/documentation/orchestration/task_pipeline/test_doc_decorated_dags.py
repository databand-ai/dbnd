# © Copyright Databand.ai, an IBM Company 2022


from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from dbnd import parameter, pipeline


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


class TestDocDecoratedDags:
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
