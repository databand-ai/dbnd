from airflow.contrib.kubernetes.kubernetes_request_factory.pod_request_factory import (
    SimplePodRequestFactory as AirflowSimplePodRequestFactory,
)
from airflow.contrib.kubernetes.secret import Secret


class DbndPodRequestFactory(AirflowSimplePodRequestFactory):
    def create(self, pod):
        req = super(DbndPodRequestFactory, self).create(pod=pod)
        self.extact_extended_resources(pod, req)
        return req

    def extact_extended_resources(self, pod, req):
        # type: (Pod, Dict) -> None
        r = pod.resources
        if not r and not r.requests and not r.limits:
            return

        req["spec"]["containers"][0].setdefault("resources", {})
        resources = req["spec"]["containers"][0]["resources"]
        if r.requests:
            resources.setdefault("requests", {})
            resources["requests"].update(**r.requests)
        if r.limits:
            resources.setdefault("limits", {})
            resources["limits"].update(**r.limits)

    def extract_node_affinity(self, pod, req):
        if not hasattr(pod, "node_affinity"):
            return

        nodeAffinity = req["spec"].setdefault("nodeSelector", {})
        nodeAffinity.update(pod.node_affinity)

    def extract_volume_secrets(self, pod, req):
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
