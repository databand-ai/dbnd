from dbnd_airflow.contants import AIRFLOW_BELOW_2


if AIRFLOW_BELOW_2:
    from airflow.jobs import BaseJob, BackfillJob, LocalTaskJob
else:
    from airflow.jobs.base_job import BaseJob
    from airflow.jobs.backfill_job import BackfillJob
    from airflow.jobs.local_task_job import LocalTaskJob
