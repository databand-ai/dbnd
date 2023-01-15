# © Copyright Databand.ai, an IBM Company 2022

"""
This file is only used in Airflow 1.10.10 and bellow;
Copied from Airflow 1.10.15 to unify secrets management
"""
import copy
import typing
import uuid

from kubernetes.client import models as k8s

from dbnd_run.airflow.compat import AIRFLOW_VERSION_2


if AIRFLOW_VERSION_2:
    from airflow.kubernetes.secret import Secret  # noqa: F401
else:
    from airflow.contrib.kubernetes.secret import Secret  # noqa: F401


def to_env_secret(secret: "Secret") -> k8s.V1EnvVar:
    """Stores es environment secret"""
    return k8s.V1EnvVar(
        name=secret.deploy_target,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(name=secret.secret, key=secret.key)
        ),
    )


def to_env_from_secret(secret: "Secret") -> k8s.V1EnvFromSource:
    """Reads from environment to secret"""
    return k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=secret.secret))


def to_volume_secret(secret: "Secret") -> typing.Tuple[k8s.V1Volume, k8s.V1VolumeMount]:
    """Converts to volume secret"""
    vol_id = f"secretvol{uuid.uuid4()}"
    volume = k8s.V1Volume(
        name=vol_id, secret=k8s.V1SecretVolumeSource(secret_name=secret.secret)
    )
    # if secret.items:
    #     volume.secret.items = self.items
    return (
        volume,
        k8s.V1VolumeMount(mount_path=secret.deploy_target, name=vol_id, read_only=True),
    )


def attach_to_pod(secret: "Secret", pod: k8s.V1Pod) -> k8s.V1Pod:
    """Attaches to pod"""
    cp_pod = copy.deepcopy(pod)
    if secret.deploy_type == "volume":
        volume, volume_mount = to_volume_secret(secret)
        cp_pod.spec.volumes = pod.spec.volumes or []
        cp_pod.spec.volumes.append(volume)
        cp_pod.spec.containers[0].volume_mounts = (
            pod.spec.containers[0].volume_mounts or []
        )
        cp_pod.spec.containers[0].volume_mounts.append(volume_mount)
    if secret.deploy_type == "env" and secret.key is not None:
        env = to_env_secret(secret)
        cp_pod.spec.containers[0].env = cp_pod.spec.containers[0].env or []
        cp_pod.spec.containers[0].env.append(env)
    if secret.deploy_type == "env" and secret.key is None:
        env_from = to_env_from_secret(secret)
        cp_pod.spec.containers[0].env_from = cp_pod.spec.containers[0].env_from or []
        cp_pod.spec.containers[0].env_from.append(env_from)
    return cp_pod
