from dbnd_airflow.compat.jobs import BackfillJob, BaseJob
from dbnd_airflow.contants import AIRFLOW_BELOW_2


AIRFLOW_BASE_JOB_CLASS = BaseJob if AIRFLOW_BELOW_2 else BackfillJob
