# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List

from dbnd import Task, parameter
from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.utils.better_subprocess import run_cmd
from dbnd._core.utils.project.project_fs import project_path


logger = logging.getLogger(__name__)


class DockerBuild(Task):
    use_kaniko = parameter(default=False)[bool]

    docker_file = parameter(default=project_path("Dockerfile"))[str]
    image_name = parameter()[str]
    tag = parameter(default="latest")[str]
    push = parameter(default=True)[bool]
    target = parameter(default=None)[str]

    working_dir = parameter(default=None)[str]

    kaniko_command = parameter(default=None)[str]
    context = parameter(default=None)[str]
    destinations = parameter(default=None)[list]

    label = parameter(default="")[str]
    platform = parameter(default="")[str]
    build_args = parameter(default=None)[List[str]]
    extra_args = parameter(default=None, description="Extra Args (any arg)")[list]

    full_image_name = None
    computed_tag = None
    image_name_with_tag = None

    def run(self):
        if self.use_kaniko:
            return self.run_using_kaniko()
        else:
            return self.run_using_docker_build()

    def run_using_docker_build(self):
        if self.tag:
            self.image_name_with_tag = "{}:{}".format(self.image_name, self.tag)
        else:
            self.image_name_with_tag = self.full_image_name

        try:
            cmd = "docker build -t {} -f {} .".format(
                self.image_name_with_tag, self.docker_file
            )
            if self.label:
                cmd = cmd + " --label " + self.label
            if self.target:
                cmd = cmd + " --target " + self.target
            if self.platform:
                # for p in self.platform:
                #     cmd = cmd + f" --platform {p}" for p in self.platform:
                cmd = cmd + f" --platform {self.platform}"
            if self.extra_args:
                cmd = cmd + " " + self.extra_args

            if self.build_args:
                build_args_list = [f" --build-arg {arg}" for arg in self.build_args]
                cmd = cmd + "".join(build_args_list)

            logger.info("Running docker build command: `%s`\n\n", cmd)
            cwd = self.working_dir or project_path()

            run_cmd(cmd, shell=True, cwd=cwd)

        except Exception as e:
            logger.error(
                "^^^^^^^^^^^^^^^ SEE DOCKER ERROR MESSAGE ABOVE THIS LINE ^^^^^^^^^^^^^^^\n\n"
            )
            raise DatabandRuntimeError(
                "failed building docker image {}".format(self.image_name_with_tag),
                nested_exceptions=[e],
            )

        if self.push:
            try:
                cmd = "docker push {}".format(self.image_name_with_tag)
                logger.info("Running docker push command: '%s'", cmd)
                run_cmd(cmd, shell=True)

            except Exception as e:
                raise DatabandRuntimeError(
                    "failed to push docker image {}".format(self.image_name_with_tag),
                    nested_exceptions=[e],
                )
        else:
            logger.info("skipping docker push")

        return self.image_name_with_tag

    def run_using_kaniko(self):
        if self.tag:
            self.image_name_with_tag = "{}:{}".format(self.image_name, self.tag)
        else:
            self.image_name_with_tag = self.full_image_name

        command = "{} -c {} -f {}".format(
            self.kaniko_command, self.context, self.docker_file
        )

        if not self.destinations:
            command = command + " --no-push"
        else:
            destination_list = [
                " -d {}".format(destination) for destination in self.destinations
            ]
            command = command + "".join(destination_list)

        if self.build_args:
            build_args_list = [" --build-arg {}".format(arg) for arg in self.build_args]
            command = command + "".join(build_args_list)

        if self.label:
            command = command + " --label " + self.label

        if self.target:
            command = command + " --target " + self.target

        try:
            logger.info("Running build using Kaniko: %s", command)
            run_cmd(command, shell=True, cwd=project_path())
        except Exception as e:
            raise DatabandRuntimeError(
                "failed building docker image {}".format("?"), nested_exceptions=[e]
            )
