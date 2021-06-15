import logging
import os

import yaml

from airflow.contrib.kubernetes.pod import Pod
from airflow.contrib.kubernetes.secret import Secret

from dbnd_airflow.compat.request_factory import serialize_pod


AIRFLOW_LOG_MOUNT_NAME = "airflow-logs"

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
        if self.kubernetes_engine_config.airflow_live_log_image is not None:
            self.add_logs_container(req)

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

    def add_logs_container(self, req):
        if self.kubernetes_engine_config.trap_exit_file_flag is None:
            logger.warning(
                "Can't use live log feature without trap exit file."
                "Please use the configuration `kubernetes.trap_exit_file_flag`"
            )

        trap_file = self.kubernetes_engine_config.trap_exit_file_flag
        trap_dir = os.path.dirname(trap_file)

        # add a new mount volume for the logs
        req["spec"].setdefault("volumes", [])
        req["spec"]["volumes"].extend(
            [
                {"name": AIRFLOW_LOG_MOUNT_NAME, "emptyDir": {}},
                {"name": "trap", "emptyDir": {}},
            ]
        )

        # add to the log mount to the original container
        req["spec"]["containers"][0].setdefault("volumeMounts", [])
        req["spec"]["containers"][0]["volumeMounts"].extend(
            [
                {
                    "name": AIRFLOW_LOG_MOUNT_NAME,
                    "mountPath": self.kubernetes_engine_config.container_airflow_log_path,
                },
                {"name": "trap", "mountPath": trap_dir,},
            ]
        )

        # configure airflow to use the ip as the hostname of the job
        # airflow-webserver uses the hostname to query the log_sidecar but in kubernetes
        # In kubernetes normally, only Services get DNS names, not Pods. so we use the ip.
        # see more here: https://stackoverflow.com/a/59262628
        if self.kubernetes_engine_config.host_as_ip_for_live_logs:
            req["spec"]["containers"][0].setdefault("env", [])
            req["spec"]["containers"][0]["env"].append(
                {
                    "name": "AIRFLOW__CORE__HOSTNAME_CALLABLE",
                    "value": "airflow.utils.net:get_host_ip_address",
                },
            )

        # build the log sidecar and add it
        log_folder = self.kubernetes_engine_config.airflow_log_folder
        airflow_image = self.kubernetes_engine_config.airflow_live_log_image
        log_port = self.kubernetes_engine_config.airflow_log_port
        side_car = {
            "name": "log-sidecar",  # keep the name lowercase
            "image": airflow_image,
            "command": ["/bin/bash", "-c"],
            "args": [
                """airflow serve_logs &
                CHILD_PID=$!
                (while true ; do if [[ -f "{0}" ]]; then kill $CHILD_PID; fi; sleep 1; done) &
                wait $CHILD_PID
                if [[ -f "{0}" ]]; then exit 0; fi""".format(
                    trap_file
                )
            ],
            # those env variable are used by `airflow serve_logs` to access the log and with the relevant port
            "env": [
                {"name": "AIRFLOW__CORE__BASE_LOG_FOLDER", "value": log_folder,},
                {"name": "AIRFLOW__CELERY__WORKER_LOG_SERVER_PORT", "value": log_port},
            ],
            "volumeMounts": [
                {"name": AIRFLOW_LOG_MOUNT_NAME, "mountPath": log_folder},
                {"name": "trap", "mountPath": trap_dir,},
            ],
            "ports": [{"containerPort": int(log_port)}],
        }

        req["spec"]["containers"].append(side_car)
