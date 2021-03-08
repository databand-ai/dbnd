from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from dbnd_airflow.scheduler.dagrun_zombies import find_and_kill_dagrun_zombies


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "execution_timeout": timedelta(minutes=5),
}

with DAG(
    dag_id="dbnd_clear_zombies",
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    description="Maintenance DAG to remove staled task (zombies)",
) as dag:
    PythonOperator(
        task_id="clear_zombies",
        python_callable=find_and_kill_dagrun_zombies,
        op_args=(None,),
    )
