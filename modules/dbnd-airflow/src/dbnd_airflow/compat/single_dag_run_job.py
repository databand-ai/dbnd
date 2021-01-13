from dbnd_airflow.compat.jobs import BackfillJob, BaseJob
from dbnd_airflow.contants import AIRFLOW_BELOW_2


AIRFLOW_BASE_JOB_CLASS = BaseJob if AIRFLOW_BELOW_2 else BackfillJob


def get_args_for_base_job_class_init(dag, *args):
    if isinstance(AIRFLOW_BASE_JOB_CLASS, BackfillJob):
        updated_args = [dag]
        updated_args.extend(args)
        return updated_args
    else:
        return args
