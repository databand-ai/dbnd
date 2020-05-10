import os

import mock

from pytest import fixture

from dbnd import dbnd_run_stop, log_metric, task
from dbnd._core.configuration import environ_config
from dbnd._core.current import try_get_databand_run
from dbnd._core.inplace_run import airflow_dag_inplace_tracking


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
    assert run.job_name == "%s.%s.fake_task_inside_dag" % (PARENT_DAG, CHILD_DAG)
    assert root_task.task_name == "fake_task_inside_dag__execute"

    return "Regular test"


patch_dict = {
    "AIRFLOW_CTX_DAG_ID": FULL_DAG_NAME,
    "AIRFLOW_CTX_TASK_ID": fake_task_inside_dag.__name__,
    "AIRFLOW_CTX_EXECUTION_DATE": "2020-04-06T14:25:00",
}


@fixture
def with_airflow_tracking_env():
    environ_config.reset()

    try:
        with mock.patch.dict(os.environ, patch_dict):
            yield
    finally:
        environ_config.reset()


class TestTaskInplaceRun(object):
    def test_sanity_with_airflow(self, with_airflow_tracking_env):
        fake_task_inside_dag()
        dbnd_run_stop()
        print("hey")
