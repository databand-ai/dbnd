# © Copyright Databand.ai, an IBM Company 2022

import os

import mock

from pytest import fixture

import dbnd._core.configuration.environ_config

from dbnd import dbnd_tracking_stop, log_metric, task
from dbnd._core.current import try_get_databand_run


PARENT_DAG = "parent_dag"
CHILD_DAG = "child_dag"
FULL_DAG_NAME = "%s.%s" % (PARENT_DAG, CHILD_DAG)


@task
def fake_task_inside_dag():
    log_metric("Testing", "Metric")
    run = try_get_databand_run()
    assert run is not None, "Task should run in databand run, check airflow tracking!"
    root_task = run.root_task

    # Validate regular subdag properties
    assert run.job_name == "%s.%s" % (PARENT_DAG, CHILD_DAG)
    # this test got problematic cause airflow_inplace task named as the script that ran it
    assert root_task.task_name

    return "Regular test"


patch_dict = {
    "AIRFLOW_CTX_DAG_ID": FULL_DAG_NAME,
    "AIRFLOW_CTX_TASK_ID": fake_task_inside_dag.__name__,
    "AIRFLOW_CTX_EXECUTION_DATE": "2020-04-06T14:25:00",
    "DBND__TRACKING": "True",
}


@fixture
def with_airflow_tracking_env():
    # we might abuse usage of some flags (like unittest mode)
    dbnd._core.configuration.environ_config.reset_dbnd_project_config()

    try:
        with mock.patch.dict(os.environ, patch_dict):
            yield
    finally:
        dbnd._core.configuration.environ_config.reset_dbnd_project_config()


class TestTaskInplaceRun(object):
    def test_sanity_with_airflow(self, with_airflow_tracking_env):
        fake_task_inside_dag()
        dbnd_tracking_stop()
