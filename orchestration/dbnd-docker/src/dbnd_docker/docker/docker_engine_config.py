# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict, List

from dbnd import parameter
from dbnd_docker.container_engine_config import ContainerEngineConfig


logger = logging.getLogger(__name__)


class DockerEngineConfig(ContainerEngineConfig):
    _conf__task_family = "docker"

    def get_docker_ctrl(self, task_run):
        from dbnd_docker.docker.docker_task_run_ctrl import LocalDockerRunCtrl

        return LocalDockerRunCtrl(task_run=task_run)

    network = parameter(default=None, description="Docker Network to connect to")[str]

    api_version = parameter(
        description="Remote API version. "
        "Set to ``auto`` to automatically detect the server's version.,"
    )[str]
    docker_url = parameter(
        description="URL of the host running the docker daemon."
    ).value("unix://var/run/docker.sock")

    auto_remove = parameter(
        description="Auto-removal of the container on daemon side when the container's process exits."
    ).value(False)
    force_pull = parameter(
        description="Pull the docker image on every run. Default is False."
    ).value(False)

    cpus = parameter(
        description="""Number of CPUs to assign to the container.
                  This value gets multiplied by 1024. See
                  https://docs.docker.com/engine/reference/run/#cpu-share-constraint"""
    ).value(1.0)

    environment = parameter.c(
        description="Environment variables to set in the container. (templated)"
    )[Dict[str, str]]
    mem_limit = parameter(
        description="""Maximum amount of memory the container can use."
               Either a float value, which represents the limit in bytes,
               or a string like ``128m`` or ``1g``."""
    ).none[str]

    network_mode = parameter(description="Network mode for the container.").none[str]

    tls_ca_cert = parameter(
        description="Path to a PEM-encoded certificate authority "
        "to secure the docker connection."
    ).none[str]

    tls_client_cert = parameter(
        description="Path to the PEM-encoded certificate used "
        "to authenticate docker client."
    ).none[str]

    tls_client_key = parameter(
        description="Path to the PEM-encoded key used " "to authenticate docker client."
    ).none[str]

    tls_hostname = parameter(
        description="Hostname to match against the docker server certificate "
        "or False to disable the check."
    ).none[str]

    tls_ssl_version = parameter(
        description="Version of SSL to use when communicating with docker daemon."
    ).none[str]

    volumes = parameter.c(
        description="List of volumes to mount into the container, "
        "e.g. `['/host/path:/container/path',"
        " '/host/path2:/container/path2:ro']`"
    )[List[str]]

    working_dir = parameter(
        description="Working directory to set on the container "
        "(equivalent to the -w switch the docker client)"
    ).none[str]

    tmp_dir = parameter(
        description="Mount point inside the container "
        "to a temporary directory created on the host "
        "by the operator. The path is also made available via the environment variable"
        " ``AIRFLOW_TMP_DIR`` inside the container."
    ).value("/tmp/airflow")

    docker_conn_id = parameter(description="ID of the Airflow connection to use").none[
        str
    ]
    user = parameter(description="Default user inside the docker container. ").none[str]
    shm_size = parameter(
        description="Size of ``/dev/shm`` in bytes. "
        "The size must be greater than 0. "
        "If omitted uses the system default."
    ).none[int]

    # xcom_all = parameter(description="Push all the stdout or just the last line. The default is False (last line).")
    # xcom_push = parameter(
    #     description="Does the stdout will be pushed to the next step using XCom. The default is False.")

    dns = parameter.c(description="Docker custom DNS servers")[List[str]]
    dns_search = parameter.c(description="Docker custom DNS search domain")[List[str]]
