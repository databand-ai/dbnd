# © Copyright Databand.ai, an IBM Company 2022

import logging
import os

from typing import List

from airflow.hooks.S3_hook import S3Hook

from dbnd import output, parameter, pipeline, task
from dbnd_aws.batch.aws_batch_ctrl import AwsBatchConfig
from dbnd_docker.docker.docker_task import DockerRunTask
from dbnd_examples.orchestration.dbnd_aws.sync_operators import S3SyncHook
from targets import target
from targets.fs import FileSystems
from targets.types import PathStr
from targets.utils.path import path_to_bucket_and_key


logger = logging.getLogger(__name__)


@task
def copy_to_s3_destination(source: List[str], destination: PathStr = output[PathStr]):
    hook = S3Hook()

    for source_file in source:
        _, source_key = path_to_bucket_and_key(source_file)
        destination_file = os.path.join(destination, source_key)

        logger.info("Copying %s -> %s", source_file, destination_file)
        if target(source_file).fs.name == FileSystems.s3:

            hook.copy_object(
                source_bucket_key=source_file, dest_bucket_key=destination_file
            )
        else:

            hook.load_file(filename=source_file, key=destination_file, replace=True)
    return source


class ValidatePartnerData(DockerRunTask):
    input_data = parameter[PathStr]
    output_validated = parameter.output[PathStr]
    command = "echo {{ params.input_data}} {{params.output_validated}} "
    image = "ubuntu:latest"

    # DockerRunTask can run on as local docker container and as  AwsBatch job.
    # Next lines define configuration parameters for AwsBatch controller
    defaults = {
        AwsBatchConfig.job_definition: "hello-world",
        AwsBatchConfig.job_queue: "first-run-job-queue",
    }

    def _task_submit(self):
        # override task submit method to add custom report after task (docker command) was executed
        v = super(ValidatePartnerData, self)._task_submit()

        self.output_validated.write("aaa")
        return v


@pipeline(destination=output)
def partner_data_ingest(
    new_files: List[str],
    partner_id=1,
    destination: PathStr = "s3://databand-victor-test/destination",
):
    """
    :param new_files : list of files to process:
    :param partner_id:
    :param destination: destination to copy validated files
    This pipeline gets list of files and run ValidatePartnerData on an every one of them, all validated files are
    then copised into a destination folder.
    """

    validated_reports = [ValidatePartnerData(input_data=f) for f in new_files]
    logger.info("%s ", validated_reports)
    published_files = copy_to_s3_destination(source=new_files, destination=destination)

    # set all validated_reports tasks as upstream for copy_to_s3_destination.
    # this would make copy_to_s3_destination only after all validated_reports finished successfully
    for vr in validated_reports:
        published_files.set_upstream(vr)

    # collect outputs (i.e. validation report) of validation tasks into a list
    reports = [vr.output_validated for vr in validated_reports]
    return published_files.result, reports


@pipeline(destination=output)
def partner_data_ingest_new_files(source, destination):
    """
    :param source : list of files to process:
    :param destination: destination to copy validated files
    check s3 path for new file, trigger partner_data_ingest for new files.
    """
    hook = S3SyncHook(aws_conn_id="aws_default", verify=True)

    diff = hook.diff(source, destination)
    return partner_data_ingest(new_files=diff, destination=destination)
