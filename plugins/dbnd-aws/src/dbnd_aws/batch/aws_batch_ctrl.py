# © Copyright Databand.ai, an IBM Company 2022

from typing import Dict

from dbnd import parameter
from dbnd._core.settings import EngineConfig
from dbnd_docker.docker_ctrl import DockerRunCtrl


class AwsBatchConfig(EngineConfig):
    """Amazon Web Services Batch"""

    _conf__task_family = "aws_batch"
    job_definition = parameter(description="the job definition name on AWS Batch").none[
        str
    ]
    overrides = parameter(
        empty_default=True,
        description="the same parameter that boto3 will receive on containerOverrides (templated)"
        " http://boto3.readthedocs.io/en/latest/reference/services/batch.html#submit_job",
    )[Dict[str, str]]
    job_queue = parameter(description="the queue name on AWS Batch")[str]
    max_retries = parameter(
        description="exponential backoff retries while waiter is not merged, 4200 = 48 hours"
    )[int]

    def get_docker_ctrl(self, task_run):
        return AWSBatchCtrl(task_run=task_run)


class AWSBatchCtrl(DockerRunCtrl):
    """
    Execute a job on AWS Batch Service
    """

    def __init__(self, **kwargs):
        super(AWSBatchCtrl, self).__init__(**kwargs)
        self.runner_op = None

    @property
    def aws_batch_config(self):
        # type: (AWSBatchCtrl) -> AwsBatchConfig
        return self.task.docker_engine

    def docker_run(self):
        dc = self.aws_batch_config

        if dc.job_definition is None:
            raise Exception("Please define aws batch definition first")
        from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator

        cloud_config = self.task.task_env
        self.runner_op = AWSBatchOperator(
            task_id=self.task_id,
            job_name=self.job.job_id,
            # per task settings
            job_definition=dc.job_definition,
            overrides=dc.overrides,
            # more global
            job_queue=dc.job_queue,
            max_retries=dc.max_retries,
            aws_conn_id=cloud_config.conn_id,
            region_name=cloud_config.region_name,
        )

        self.runner_op.execute(context=None)

    def on_kill(self):
        if self.runner_op is not None:
            self.runner_op.on_kill()
