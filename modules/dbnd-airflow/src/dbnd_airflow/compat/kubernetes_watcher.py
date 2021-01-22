from dbnd_airflow.contants import AIRFLOW_BELOW_2


def get_run_watcher_args(kube_client, watcher_obj):
    if AIRFLOW_BELOW_2:
        return (
            kube_client,
            watcher_obj.resource_version,
            watcher_obj.worker_uuid,
            watcher_obj.kube_config,
        )
    return (
        kube_client,
        watcher_obj.resource_version,
        watcher_obj.scheduler_job_id,
        watcher_obj.kube_config,
    )
