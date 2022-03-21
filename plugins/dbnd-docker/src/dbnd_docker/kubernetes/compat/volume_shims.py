import copy
import typing

from kubernetes.client import models as k8s


def to_k8s_client_obj(volume_spec: typing.Dict[str, typing.Any]) -> k8s.V1Volume:
    name = volume_spec.get("name")

    volume = k8s.V1Volume(name=name)
    for k, v in volume_spec.items():
        snake_key = _convert_to_snake_case(k)
        if hasattr(volume, snake_key):
            setattr(volume, snake_key, v)
        else:
            raise AttributeError("k8s.V1Volume does not have attribute {}".format(k))
    return volume


def attach_to_pod(
    pod: k8s.V1Pod, volume_spec: typing.Dict[str, typing.Any]
) -> k8s.V1Pod:
    cp_pod = copy.deepcopy(pod)
    volume = to_k8s_client_obj(volume_spec)
    cp_pod.spec.volumes = pod.spec.volumes or []
    cp_pod.spec.volumes.append(volume)
    return cp_pod


def _convert_to_snake_case(some_string):
    return "".join(["_" + i.lower() if i.isupper() else i for i in some_string]).lstrip(
        "_"
    )


def attach_volume_mount(
    pod: k8s.V1Pod, volume_mount_spec: typing.Dict[str, typing.Any]
):
    cp_pod = copy.deepcopy(pod)

    volume_mount = k8s.V1VolumeMount(
        name=volume_mount_spec.get("name"),
        mount_path=volume_mount_spec.get("mountPath"),
        sub_path=volume_mount_spec.get("subPath"),
        read_only=volume_mount_spec.get("readOnly"),
    )
    cp_pod.spec.containers[0].volume_mounts = pod.spec.containers[0].volume_mounts or []
    cp_pod.spec.containers[0].volume_mounts.append(volume_mount)
    return cp_pod
