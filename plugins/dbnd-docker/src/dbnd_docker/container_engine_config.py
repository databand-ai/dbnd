# © Copyright Databand.ai, an IBM Company 2022

import subprocess

from dbnd import parameter
from dbnd._core.parameter.validators import NonEmptyString
from dbnd._core.run.databand_run import DatabandRun
from dbnd._core.settings import EngineConfig
from targets.values.version_value import VersionStr


class ContainerEngineConfig(EngineConfig):
    require_submit = True
    dbnd_executable = ["dbnd"]  # we should have 'dbnd' command installed in container
    container_repository = parameter(validator=NonEmptyString()).help(
        "Docker container registry"
    )[str]
    container_tag = parameter.none().help("Docker container tag")[VersionStr]
    container_tag_gpu = parameter.none().help("Docker container tag for GPU tasks")[
        VersionStr
    ]

    docker_build_tag_base = parameter.help("Auto build docker container tag").value(
        "dbnd_build"
    )
    docker_build_tag = parameter.help(
        "Docker build tag for the docker image dbnd will build"
    ).default(None)[str]
    docker_build = parameter(default=True).help(
        "Automatically build docker image. "
        "If container_repository is unset it will be taken (along with the tag) from the docker build settings"
    )[bool]
    docker_build_push = parameter(default=True).help(
        "If docker_build is enabled, controls whether the image is automatically pushed or not"
    )

    def get_docker_ctrl(self, task_run):
        pass

    @property
    def full_image(self):
        return "{}:{}".format(self.container_repository, self.container_tag)

    def prepare_for_run(self, run):
        # type: (DatabandRun) -> None
        super(ContainerEngineConfig, self).prepare_for_run(run)

        from dbnd_docker.submit_ctrl import prepare_docker_for_executor

        # when we run at submitter - we need to update driver_engine - this one will be used to send job
        # when we run at driver - we update task config, it will be used by task
        # inside pod submission the fallback is always on task_engine

        prepare_docker_for_executor(run, self)

    def submit_to_engine_task(self, env, task_name, args, interactive=True):
        from dbnd_docker.docker.docker_task import DockerRunTask

        submit_task = DockerRunTask(
            task_name=task_name,
            command=subprocess.list2cmdline(args),
            image=self.full_image,
            docker_engine=self,
            task_is_system=True,
        )
        return submit_task

    def _should_wrap_with_submit_task(self, task_run):
        """
        We don't want to resubmit if it's dockerized run and we running with the same engine
        """
        from dbnd_docker.docker.docker_task import DockerRunTask

        if isinstance(task_run.task, DockerRunTask):
            if task_run.task.docker_engine.task_name == self.task_name:
                return False
        return super(ContainerEngineConfig, self)._should_wrap_with_submit_task(
            task_run
        )
