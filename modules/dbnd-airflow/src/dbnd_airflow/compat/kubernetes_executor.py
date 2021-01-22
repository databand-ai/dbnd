import airflow

from airflow import AirflowException

from dbnd._core.current import try_get_databand_run
from dbnd_airflow._vendor import kubernetes_utils
from dbnd_airflow.constants import AIRFLOW_ABOVE_9, AIRFLOW_ABOVE_10
from dbnd_airflow.contants import AIRFLOW_BELOW_2


if AIRFLOW_ABOVE_10:
    from airflow.executors.kubernetes_executor import (
        AirflowKubernetesScheduler,
        KubernetesJobWatcher,
        KubernetesExecutor,
        KubeConfig,
    )
else:
    from airflow.contrib.executors.kubernetes_executor import (
        AirflowKubernetesScheduler,
        KubernetesJobWatcher,
        KubernetesExecutor,
        KubeConfig,
    )


def make_safe_label_value(value):
    return kubernetes_utils.make_safe_label_value(value)


def get_tuple_for_watcher_queue(
    pod_id, namespace, state, labels, annotations, resource_version
):
    if not AIRFLOW_BELOW_2:
        return pod_id, namespace, state, annotations, resource_version

    if AIRFLOW_ABOVE_9:
        return pod_id, namespace, state, labels, resource_version
    return pod_id, state, labels, resource_version


def get_job_watcher_kwargs(dbnd_kubernetes_scheduler):
    kwargs = {
        "namespace": dbnd_kubernetes_scheduler.namespace,
        "watcher_queue": dbnd_kubernetes_scheduler.watcher_queue,
        "resource_version": dbnd_kubernetes_scheduler.current_resource_version,
        "worker_uuid"
        if AIRFLOW_BELOW_2
        else "scheduler_job_id": dbnd_kubernetes_scheduler.scheduler_job_id_or_worker_uuid,
        "kube_config": dbnd_kubernetes_scheduler.kube_config,
        "kube_dbnd": dbnd_kubernetes_scheduler.kube_dbnd,
    }
    if AIRFLOW_ABOVE_10:
        kwargs.update({"multi_namespace_mode": False})

    return kwargs


def get_worker_uuid_or_scheduler_job_id(kube_executor):
    dbnd_run = try_get_databand_run()
    if AIRFLOW_BELOW_2:
        if dbnd_run:
            return str(dbnd_run.run_uid)
        else:
            return (
                airflow.models.KubeWorkerIdentifier.get_or_create_current_kube_worker_uuid()
            )
    else:
        if not kube_executor.job_id:
            raise AirflowException("Could not get scheduler_job_id")
        return kube_executor.job_id


def get_safe_execution_datestring(execution_date):
    if AIRFLOW_BELOW_2:
        return AirflowKubernetesScheduler._datetime_to_label_safe_datestring(
            execution_date
        )
    return airflow.kubernetes.pod_generator.datetime_to_label_safe_datestring(
        execution_date
    )


def get_change_state_args(key, state, pod_id, namespace):
    if AIRFLOW_BELOW_2:
        return key, state, pod_id
    return key, state, pod_id, namespace
