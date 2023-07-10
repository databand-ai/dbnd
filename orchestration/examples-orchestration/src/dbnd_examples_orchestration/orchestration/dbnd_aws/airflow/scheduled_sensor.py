# © Copyright Databand.ai, an IBM Company 2022

from datetime import datetime, timedelta

from airflow import DAG
from dbnd_examples_orchestration.orchestration.dbnd_aws import (
    partner_data_ingest_new_files,
)
from dbnd_examples_orchestration.orchestration.dbnd_aws.sync_operators import (
    S3StatefulSensor,
)

from dbnd_run.airflow.scheduler.dbnd_cmd_operators import dbnd_task_as_bash_operator


"""
    this file should be put to airflow dag folder (or dag folder should point here)
    It contains definition of Airflow Dag that is used to periodically schedule databand runs.
    A scheduled_s3_sensor DAG is running Airflow sensor that waits and fires event when new files
    are added to source path (new means it does not exists in destination path). ingest_op is running as downstream
    task and ingest new files after validation into destination path. This DAG is scheduled to run every 2 minutes
    note that ingest_op is defines as databand task and used as Airflow operator.
    """

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(year=2019, month=5, day=23, hour=13),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "params": {
        "source": "s3://databand-victor-test/sensor2/*.csv",
        "destination": "s3://victor-test-target/tmp/3",
    }
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

with DAG(
    "scheduled_s3_sensor",
    schedule_interval=timedelta(minutes=2),
    max_active_runs=1,
    default_args=default_args,
) as dag:

    sensor = S3StatefulSensor(
        task_id="trigger_on_new_files",
        source="{{params.source}}",
        destination="{{params.destination}}",
        timeout=18 * 60 * 60,
        poke_interval=120,
        dag=dag,
    )

    ingest_op = dbnd_task_as_bash_operator(
        partner_data_ingest_new_files,
        cmd_line="--task-target-date {{ ds }} "
        "--source '{{params.source}}' "
        "--destination '{{params.destination}}' ",
    )

    ingest_op.set_upstream(sensor)
