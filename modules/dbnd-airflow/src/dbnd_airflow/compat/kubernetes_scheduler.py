from dbnd_airflow.contants import AIRFLOW_BELOW_2


def unpack_next_job(next_job):
    if AIRFLOW_BELOW_2:
        key, command, kube_executor_config = next_job
        return key, command, kube_executor_config, None
    return next_job
