# © Copyright Databand.ai, an IBM Company 2022

from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from airflow.utils.dates import days_ago

from dbnd_airflow import track_task


DEFAULT_DATE = timezone.datetime(2015, 1, 1)


def _test_func(**kwargs):
    return "funcreturnvalue"


class TestTrackOperator(object):
    def test_track_python_operator(self):
        args = dict(start_date=days_ago(2))

        with DAG(
            dag_id="test_dag", default_args=args, schedule_interval=timedelta(minutes=1)
        ):
            run_this = PythonOperator(
                task_id="print_the_context",
                provide_context=True,
                python_callable=_test_func,
            )
        track_task(run_this)
        #
        # env = {
        #     "AIRFLOW_CTX_DAG_ID": "test_dag",
        #     "AIRFLOW_CTX_EXECUTION_DATE": "emr_task",
        #     "AIRFLOW_CTX_TASK_ID": "1970-01-01T0000.000",
        #     "AIRFLOW_CTX_TRY_NUMBER": "1",
        #     "AIRFLOW_CTX_UID": get_airflow_instance_uid(),
        # }
        #
        # with mock.patch.dict(os.environ, env):

        run_this.run(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True
        )
