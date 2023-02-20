# © Copyright Databand.ai, an IBM Company 2022

import logging
import os

from typing import Any, Dict


AIRFLOW_LOG_MOUNT_NAME = "airflow-logs"

logger = logging.getLogger(__name__)


class DbndPodRequestFactory(object):
    def __init__(self, kubernetes_engine_config):
        self.kubernetes_engine_config = kubernetes_engine_config

    def attach_logs_container(self, mutable_pod_creation_request: Dict[str, Any]):
        if self.kubernetes_engine_config.airflow_log_enabled:
            self.add_logs_container(mutable_pod_creation_request)

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
        base_container = req["spec"]["containers"][0]
        base_container.setdefault("volumeMounts", [])
        base_container["volumeMounts"].extend(
            [
                {
                    "name": AIRFLOW_LOG_MOUNT_NAME,
                    "mountPath": self.kubernetes_engine_config.container_airflow_log_path,
                },
                {"name": "trap", "mountPath": trap_dir},
            ]
        )

        # configure airflow to use the ip as the hostname of the job
        # airflow-webserver uses the hostname to query the log_sidecar but in kubernetes
        # In kubernetes normally, only Services get DNS names, not Pods. so we use the ip.
        # see more here: https://stackoverflow.com/a/59262628
        if self.kubernetes_engine_config.host_as_ip_for_live_logs:
            base_container.setdefault("env", [])
            base_container["env"].append(
                {
                    "name": "AIRFLOW__CORE__HOSTNAME_CALLABLE",
                    "value": "airflow.utils.net:get_host_ip_address",
                }
            )

        # build the log sidecar and add it
        log_folder = self.kubernetes_engine_config.airflow_log_folder

        airflow_image = (
            self.kubernetes_engine_config.airflow_log_image or base_container["image"]
        )
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
            "volumeMounts": [
                {"name": AIRFLOW_LOG_MOUNT_NAME, "mountPath": log_folder},
                {"name": "trap", "mountPath": trap_dir},
            ],
            "ports": [{"containerPort": int(log_port)}],
        }

        # we want our side container to have basic env
        side_car["env"] = (
            list(base_container["env"]) if base_container.get("env") else []
        )
        existing_keys = {env_def.get("name") for env_def in side_car["env"]}

        # those env variable are used by `airflow serve_logs` to access the log and with the relevant port
        if "AIRFLOW__CORE__BASE_LOG_FOLDER" not in existing_keys:
            side_car["env"].append(
                {"name": "AIRFLOW__CORE__BASE_LOG_FOLDER", "value": log_folder}
            )
        if "AIRFLOW__CELERY__WORKER_LOG_SERVER_PORT" not in existing_keys:
            side_car["env"].append(
                {"name": "AIRFLOW__CELERY__WORKER_LOG_SERVER_PORT", "value": log_port}
            )

        req["spec"]["containers"].append(side_car)
