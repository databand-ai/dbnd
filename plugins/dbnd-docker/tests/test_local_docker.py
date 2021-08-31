from __future__ import absolute_import

import pytest

from dbnd_docker.docker.docker_engine_config import DockerEngineConfig
from dbnd_docker.docker.docker_task import DockerRunTask


@pytest.mark.docker
class TestLocalDocker(object):
    def test_simple_docker(self):
        t = DockerRunTask(
            command="env",
            image="ubuntu:latest",
            override={DockerEngineConfig.environment: {"UNIT": "TEST"}},
        )
        t.dbnd_run()
