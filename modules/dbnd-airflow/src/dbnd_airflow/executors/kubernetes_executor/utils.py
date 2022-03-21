import typing


if typing.TYPE_CHECKING:
    from airflow.contrib.executors.kubernetes_executor import KubeConfig

    from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig


def _update_airflow_kube_config(
    airflow_kube_config: "KubeConfig", engine_config: "KubernetesEngineConfig"
) -> None:
    # We (almost) don't need this mapping any more
    # Pod is created using  databand KubeConfig
    # The only things to patch now is `namespace` and `pods_creation_batch_size`
    ec = engine_config

    if ec.pods_creation_batch_size is not None:
        airflow_kube_config.worker_pods_creation_batch_size = (
            ec.pods_creation_batch_size
        )

    if ec.namespace is not None:
        # used in KubeWatcher internally and will watch `default` namespace if we do not update
        airflow_kube_config.kube_namespace = ec.namespace
        airflow_kube_config.executor_namespace = ec.namespace
