import logging

from distutils.version import LooseVersion

import airflow
import yaml

from airflow.contrib.kubernetes.pod import Pod
from airflow.contrib.kubernetes.secret import Secret

from dbnd_airflow.compat.request_factory import serialize_pod


logger = logging.getLogger(__name__)


class DbndPodRequestFactory(object):
    def __init__(self, kubernetes_engine_config, pod_yaml):
        self.kubernetes_engine_config = kubernetes_engine_config
        self.yaml = yaml.safe_load(pod_yaml) or {}

    def create(self, pod):

        req = serialize_pod(pod, self.kubernetes_engine_config)

        self.extract_node_affinity(pod, req)
        self.extract_volume_secrets(pod, req)
        self.extract_extended_resources(req, pod)
        self.extract_restart_policy(req)

        return req

    def extract_restart_policy(self, req):
        restart_policy = self.yaml["spec"]["restartPolicy"]
        req["spec"].setdefault("restartPolicy", restart_policy)

    def extract_extended_resources(self, req, pod):
        # type: (Dict, Pod) -> None
        limits = getattr(pod.resources, "limits", self.kubernetes_engine_config.limits)
        requests = getattr(
            pod.resources, "requests", self.kubernetes_engine_config.requests
        )

        if not any((limits, requests)):
            return

        req["spec"]["containers"][0].setdefault("resources", {})
        resources = req["spec"]["containers"][0]["resources"]

        if requests:
            resources.setdefault("requests", {})
            resources["requests"].update(**requests)

        if limits:
            resources.setdefault("limits", {})
            resources["limits"].update(**limits)

    @staticmethod
    def extract_node_affinity(pod, req):
        if not hasattr(pod, "node_affinity"):
            return

        nodeAffinity = req["spec"].setdefault("nodeSelector", {})
        nodeAffinity.update(pod.node_affinity)

    @staticmethod
    def extract_volume_secrets(pod, req):
        vol_secrets = [s for s in pod.secrets if s.deploy_type == "volume"]
        if any(vol_secrets):
            req["spec"]["containers"][0]["volumeMounts"] = req["spec"]["containers"][
                0
            ].get("volumeMounts", [])
            req["spec"]["volumes"] = req["spec"].get("volumes", [])
        for idx, vol in enumerate(vol_secrets):  # type: Secret
            vol_id = "secretvol" + str(idx)
            volumeMount = {
                "mountPath": vol.deploy_target,
                "name": vol_id,
                "readOnly": True,
            }
            if vol.key:
                volumeMount["subPath"] = vol.key
            req["spec"]["containers"][0]["volumeMounts"].append(volumeMount)
            req["spec"]["volumes"].append(
                {"name": vol_id, "secret": {"secretName": vol.secret}}
            )
