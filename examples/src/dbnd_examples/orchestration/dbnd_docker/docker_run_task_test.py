from __future__ import absolute_import

from dbnd_docker.docker.docker_task import DockerRunTask


class SimpleDockerizedPythonTask(DockerRunTask):
    command = "python -c 'import time;print(time.time())';time.sleep(100)"
    image = "python:3.7-slim"
