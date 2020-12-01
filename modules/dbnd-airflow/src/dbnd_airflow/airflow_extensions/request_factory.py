import logging
import pprint

from distutils.version import LooseVersion

import airflow
import yaml

from airflow.contrib.kubernetes.secret import Secret


logger = logging.getLogger(__name__)


class DbndPodRequestFactory(object):
    def __init__(self, kubernetes_engine_config, pod_yaml):
        self.kubernetes_engine_config = kubernetes_engine_config
        self.yaml = yaml.safe_load(pod_yaml) or {}

    def create(self, pod):
        original_pod = pod

        if LooseVersion(airflow.version.version) > LooseVersion("1.10.10"):
            pod = pod.to_v1_kubernetes_pod()

        req = self.get_pod_serializer()(pod)

        self.extract_node_affinity(original_pod, req)
        self.extract_volume_secrets(original_pod, req)
        self.extract_extended_resources(req)
        self.extract_restart_policy(req)
        logging.info("Created pod request: %s" % pprint.pprint(req))

        return req

    def extract_restart_policy(self, req):
        restart_policy = self.yaml["spec"]["restartPolicy"]
        req["spec"].setdefault("restartPolicy", restart_policy)

    def extract_extended_resources(self, req):
        # type: (Dict) -> None
        limits = self.kubernetes_engine_config.limits
        requests = self.kubernetes_engine_config.requests

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

    @staticmethod
    def get_pod_serializer():
        # constraint is set to >1.10.10 but remember that kubernetes module
        # should never be run with airflow==1.10.11 as it is broken
        if LooseVersion(airflow.version.version) > LooseVersion("1.10.10"):
            from airflow.kubernetes.kube_client import get_kube_client

            # airflow>1.10.10 uses official kubernetes client (https://github.com/kubernetes-client/python/)
            # to create pod json request instead of custom SimplePodRequest class
            kube_client = get_kube_client()
            return kube_client.api_client.sanitize_for_serialization

        from airflow.contrib.kubernetes.kubernetes_request_factory.pod_request_factory import (
            SimplePodRequestFactory as AirflowSimplePodRequestFactory,
        )

        return AirflowSimplePodRequestFactory().create

    @staticmethod
    def get_pod_resources(pod):
        if LooseVersion(airflow.version.version) > LooseVersion("1.10.10"):
            return pod.spec.containers[0].resources
        return pod.resources
