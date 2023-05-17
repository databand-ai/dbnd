# © Copyright Databand.ai, an IBM Company 2022

import logging

import dbnd

from dbnd import register_config_cls
from dbnd._core.context.use_dbnd_run import is_airflow_package_installed


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_setup_plugin():
    from dbnd_docker.docker.docker_engine_config import DockerEngineConfig
    from dbnd_docker.docker.docker_task import DockerRunTask

    register_config_cls(DockerEngineConfig)
    register_config_cls(DockerRunTask)
    if is_airflow_package_installed():
        from dbnd_docker.kubernetes.kubernetes_engine_config import (
            KubernetesEngineConfig,
        )

        register_config_cls(KubernetesEngineConfig)
    logger.debug("Registered kubernetes plugin")
