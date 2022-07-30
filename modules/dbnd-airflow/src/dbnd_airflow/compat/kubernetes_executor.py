# © Copyright Databand.ai, an IBM Company 2022

import typing

from dbnd_airflow._vendor import kubernetes_utils
from dbnd_airflow.constants import AIRFLOW_ABOVE_10, AIRFLOW_VERSION_2


if typing.TYPE_CHECKING:
    from dbnd_airflow.executors.kubernetes_executor.kubernetes_scheduler import (
        DbndKubernetesScheduler,
    )

if AIRFLOW_ABOVE_10:
    from airflow.executors.kubernetes_executor import (  # noqa: F401
        AirflowKubernetesScheduler,
        KubeConfig,
        KubernetesExecutor,
        KubernetesJobWatcher,
    )
else:
    from airflow.contrib.executors.kubernetes_executor import (  # noqa: F401
        AirflowKubernetesScheduler,
        KubeConfig,
        KubernetesExecutor,
        KubernetesJobWatcher,
    )


def make_safe_label_value(value):
    return kubernetes_utils.make_safe_label_value(value)


def get_job_watcher_kwargs(dbnd_kubernetes_scheduler: "DbndKubernetesScheduler"):

    kwargs = {
        "namespace": dbnd_kubernetes_scheduler.namespace,
        "watcher_queue": dbnd_kubernetes_scheduler.watcher_queue,
        "resource_version": dbnd_kubernetes_scheduler.current_resource_version,
        "kube_config": dbnd_kubernetes_scheduler.kube_config,
        "kube_dbnd": dbnd_kubernetes_scheduler.kube_dbnd,
    }
    if AIRFLOW_ABOVE_10:
        kwargs.update({"multi_namespace_mode": False})
    if AIRFLOW_VERSION_2:
        kwargs.update({"scheduler_job_id": dbnd_kubernetes_scheduler.scheduler_job_id})
    else:
        kwargs.update({"worker_uuid": dbnd_kubernetes_scheduler.worker_uuid})

    return kwargs
