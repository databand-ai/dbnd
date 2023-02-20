# © Copyright Databand.ai, an IBM Company 2022

from __future__ import absolute_import

import pytest

from dbnd._core.constants import CloudType
from dbnd_aws.batch.aws_batch_ctrl import AwsBatchConfig
from dbnd_docker.docker.docker_task import DockerRunTask


@pytest.mark.aws
@pytest.mark.awsbatch
class TestAwsBatch(object):
    def test_aws_batch_hello_world_task(self):
        t = DockerRunTask(
            image=None,
            command="not in use",
            override={
                AwsBatchConfig.job_definition: "hello-world",
                AwsBatchConfig.job_queue: "first-run-job-queue",
                AwsBatchConfig.max_retries: 5,
            },
            task_env=CloudType.aws,
        )
        t.dbnd_run()
