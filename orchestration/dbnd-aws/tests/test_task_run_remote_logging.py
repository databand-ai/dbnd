# © Copyright Databand.ai, an IBM Company 2022

import logging

import pytest
import six

from pytest import fixture

from dbnd import task


logger = logging.getLogger(__name__)


@fixture
def s3_logging_path(s3_path):
    return "{}/test_remote_logging/".format(s3_path)


@fixture
def databand_context_kwargs(s3_logging_path):
    return dict(conf={"aws": {"root": s3_logging_path}})


@task
def task_test_logging():
    logger.info("this is ground control to major tom")


@pytest.mark.aws
@pytest.mark.skip
def test_remote_s3_log():
    run = task_test_logging.dbnd_run(task_env="aws", task_version="now")

    remote_log_body = list(run.task_runs_by_id.values())[0].log.remote_log_file.read()
    if six.PY2:
        remote_log_body = remote_log_body.decode("utf-8")

    logger.info("remote_log_body:\n%s\n", remote_log_body)

    assert remote_log_body and len(remote_log_body) > 0
