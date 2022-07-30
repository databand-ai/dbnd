# © Copyright Databand.ai, an IBM Company 2022

"""
Code that goes along with the Airflow tutorial located at:
https://github.com/airbnb/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from dbnd.tasks.basics import dbnd_sanity_check
from dbnd_airflow.functional.dbnd_cmd_operators import dbnd_task_as_bash_operator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "schedule_interval": timedelta(minutes=1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

with DAG("scheduled_dbnd_check", default_args=default_args) as dag:
    templated_command = (
        "dbnd run dbnd_sanity_check --task-target-date {{ ds }} "
        "--name {{ params.my_param }}"
    )
    BashOperator(
        task_id="run_dbnd_check",
        bash_command=templated_command,
        params={"my_param": "name_from_params"},
        dag=dag,
    )

every_minute = dbnd_task_as_bash_operator(
    dbnd_sanity_check,
    name="dbnd_check_every_minute",
    schedule_interval=timedelta(minutes=2),
    default_args=default_args,
)

print(every_minute)
