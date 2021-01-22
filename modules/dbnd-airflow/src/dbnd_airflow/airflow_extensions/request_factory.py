import logging

import yaml


logger = logging.getLogger(__name__)


class DbndPodRequestFactory(object):
    def __init__(self, kubernetes_engine_config, pod_yaml):
        self.kubernetes_engine_config = kubernetes_engine_config
        self.yaml = yaml.safe_load(pod_yaml) or {}

    def create(self, pod):

        req = self.serialize_pod(pod, self.kubernetes_engine_config)

        self.extract_node_affinity(pod, req)
        self.extract_extended_resources(req)
        self.extract_restart_policy(req)

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
        if not hasattr(pod.spec, "node_affinity"):
            return

        nodeAffinity = req["spec"].setdefault("nodeSelector", {})
        nodeAffinity.update(pod.node_affinity)

    @staticmethod
    def serialize_pod(pod, engine_config):
        from airflow.kubernetes.kube_client import get_kube_client

        kube_client = get_kube_client(in_cluster=engine_config.in_cluster)
        return kube_client.api_client.sanitize_for_serialization(pod)
