import logging
import typing

from typing import Type

from dbnd import dbnd_config, override
from dbnd._core.commands import log_metric
from dbnd._core.constants import CURRENT_TIME_STR
from dbnd._core.errors.friendly_error.executor_k8s import no_tag_on_no_build
from dbnd._core.plugin.dbnd_plugins import pm
from dbnd_docker.container_engine_config import ContainerEngineConfig
from dbnd_docker.docker.docker_build import DockerBuild
from dbnd_docker.kubernetes.kubernetes_engine_config import KubernetesEngineConfig


if typing.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def prepare_docker_for_executor(run, docker_engine):
    if docker_engine.container_tag:
        logger.info(
            "Omitting docker build due to existing container_tag=%s",
            docker_engine.container_tag,
        )
        log_metric("container_tag", docker_engine.container_tag)
        return None

    config_cls = docker_engine.__class__  # type: Type[ContainerEngineConfig]

    def _set_config(parameter, value):
        # set value in already existing object
        setattr(docker_engine, parameter.name, value)
        dbnd_config.set_parameter(
            parameter, override(value), source="prepare_docker_for_executor"
        )

    if docker_engine.docker_build:
        auto_tag = docker_engine.docker_build_tag + "_" + CURRENT_TIME_STR
        _set_config(config_cls.container_tag, auto_tag)

        if (
            isinstance(docker_engine, KubernetesEngineConfig)
            and not docker_engine.docker_build_push
        ):
            _set_config(config_cls.image_pull_policy, "Never")

        log_metric("docker build tag", auto_tag)
        log_metric("container_tag", auto_tag)
        docker_build = DockerBuild(
            task_name="dbnd_image_build",
            image_name=docker_engine.container_repository,
            tag=docker_engine.container_tag,
            push=docker_engine.docker_build_push,
            task_version="now",
            task_is_system=True,
        )

        run.run_dynamic_task(docker_build)
        pm.hook.dbnd_build_project_docker(
            docker_engine=docker_engine, docker_build_task=docker_build
        )

        return docker_build
    else:
        logger.info("Omitting docker build due to docker_build=False")
        if not docker_engine.container_tag:
            raise no_tag_on_no_build()
    return None
