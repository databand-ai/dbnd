from dbnd_airflow._vendor import kubernetes_utils
from dbnd_airflow.constants import AIRFLOW_ABOVE_9, AIRFLOW_ABOVE_10


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


def get_tuple_for_watcher_queue(pod_id, namespace, state, labels, resource_version):
    if AIRFLOW_ABOVE_9:
        return pod_id, namespace, state, labels, resource_version
    return pod_id, state, labels, resource_version


def get_job_watcher_kwargs(dbnd_kubernetes_scheduler):
    kwargs = {
        "namespace": dbnd_kubernetes_scheduler.namespace,
        "watcher_queue": dbnd_kubernetes_scheduler.watcher_queue,
        "resource_version": dbnd_kubernetes_scheduler.current_resource_version,
        "worker_uuid": dbnd_kubernetes_scheduler.worker_uuid,
        "kube_config": dbnd_kubernetes_scheduler.kube_config,
        "kube_dbnd": dbnd_kubernetes_scheduler.kube_dbnd,
    }
    if AIRFLOW_ABOVE_10:
        kwargs.update({"multi_namespace_mode": False})

    return kwargs
