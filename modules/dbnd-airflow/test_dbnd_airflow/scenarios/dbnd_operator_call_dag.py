"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("dbnd_operator_execute", default_args=default_args)


class DbndOperator(BaseOperator):
    """
    This is the Airflow operator that is created for every Databand Task
    """

    @apply_defaults
    def __init__(self, dbnd_task_id, **kwargs):
        super(DbndOperator, self).__init__(**kwargs)
        self.dbnd_task_id = dbnd_task_id

    def execute(self, context):
        from dbnd_airflow.dbnd_task_executor.dbnd_execute import dbnd_operator__execute

        return dbnd_operator__execute(self, context)


# We want to run dbnd code directly from operator
# this way we can just run it via `airflow test ...`
dbnd_task_id = "dbnd_sanity_check__6f71842fcd"
dbnd_driver_dump = "data/dbnd/log/2020-01-04/2020-01-04T143805.017866_dbnd_sanity_check_sad_spence/dbnd_driver.pickle"

executor_config = {"DatabandExecutor": {"dbnd_driver_dump": dbnd_driver_dump}}
t1 = DbndOperator(
    task_id="dbnd_operator",
    dbnd_task_id=dbnd_task_id,
    dag=dag,
    executor_config=executor_config,
)
