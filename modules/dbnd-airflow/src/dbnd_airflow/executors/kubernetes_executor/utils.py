import logging
import os
import signal
import typing

from dbnd._core.utils.basics.signal_utils import safe_signal


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from airflow.contrib.executors.kubernetes_executor import KubeConfig
    from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig


def _update_airflow_kube_config(airflow_kube_config, engine_config):
    # type:( KubeConfig, KubernetesEngineConfig) -> None
    # We (almost) don't need this mapping any more
    # Pod is created using  databand KubeConfig
    # We still are mapping databand KubeConfig -> airflow KubeConfig as some functions are using values from it.
    ec = engine_config

    secrets = ec.get_secrets(include_system_secrets=True)
    if secrets:
        kube_secrets = {}
        env_from_secret_ref = []
        for s in secrets:
            if s.deploy_type == "env":
                if s.deploy_target:
                    kube_secrets[s.deploy_target] = "%s=%s" % (s.secret, s.key)
                else:
                    env_from_secret_ref.append(s.secret)

        if kube_secrets:
            airflow_kube_config.kube_secrets.update(kube_secrets)

        if env_from_secret_ref:
            airflow_kube_config.env_from_secret_ref = ",".join(env_from_secret_ref)

    if ec.env_vars is not None:
        airflow_kube_config.kube_env_vars.update(ec.env_vars)

    if ec.configmaps is not None:
        airflow_kube_config.env_from_configmap_ref = ",".join(ec.configmaps)

    if ec.container_repository is not None:
        airflow_kube_config.worker_container_repository = ec.container_repository
    if ec.container_tag is not None:
        airflow_kube_config.worker_container_tag = ec.container_tag
    airflow_kube_config.kube_image = "{}:{}".format(
        airflow_kube_config.worker_container_repository,
        airflow_kube_config.worker_container_tag,
    )

    if ec.image_pull_policy is not None:
        airflow_kube_config.kube_image_pull_policy = ec.image_pull_policy
    if ec.node_selectors is not None:
        airflow_kube_config.kube_node_selectors.update(ec.node_selectors)
    if ec.annotations is not None and airflow_kube_config.kube_annotations is not None:
        airflow_kube_config.kube_annotations.update(ec.annotations)

    if ec.pods_creation_batch_size is not None:
        airflow_kube_config.worker_pods_creation_batch_size = (
            ec.pods_creation_batch_size
        )
    if ec.service_account_name is not None:
        airflow_kube_config.worker_service_account_name = ec.service_account_name
    if ec.image_pull_secrets is not None:
        airflow_kube_config.image_pull_secrets = ec.image_pull_secrets

    if ec.namespace is not None:
        airflow_kube_config.kube_namespace = ec.namespace
    if ec.namespace is not None:
        airflow_kube_config.executor_namespace = ec.namespace

    if ec.gcp_service_account_keys is not None:
        airflow_kube_config.gcp_service_account_keys = ec.gcp_service_account_keys
    if ec.affinity is not None:
        airflow_kube_config.kube_affinity = ec.affinity
    if ec.tolerations is not None:
        airflow_kube_config.kube_tolerations = ec.tolerations


def mgr_sig_handler(signal, frame):
    logger.error(
        "Kubernetes python SyncManager got SIGINT (waiting for .stop command). PID: %s",
        os.getpid(),
    )


def mgr_init():
    safe_signal(signal.SIGINT, mgr_sig_handler)
