import os

import mock

from pytest import fixture

from dbnd import log_metric, task
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
    assert run.job_name == PARENT_DAG
    assert root_task.task_name == "DAG__runtime"

    # Validate relationships
    ## sub dag
    child_task = list(root_task.task_dag.upstream)[0]
    assert "fake_task_inside_dag" in child_task.task_name
    assert child_task.dag_id == FULL_DAG_NAME
    ## function task
    func_task = list(child_task.task_dag.upstream)[0]
    assert fake_task_inside_dag.__name__ in func_task.task_name

    return "Regular test"


patch_dict = {
    "AIRFLOW_CTX_DAG_ID": FULL_DAG_NAME,
    "AIRFLOW_CTX_TASK_ID": fake_task_inside_dag.__name__,
    "AIRFLOW_CTX_EXECUTION_DATE": "2020-04-06T14:25:00",
}


@fixture
def set_env():
    with mock.patch.dict(os.environ, patch_dict):
        yield


class TestTaskInplaceRun(object):
    def test_sanity_with_airflow(self, set_env):
        airflow_dag_inplace_tracking._TRY_GET_AIRFLOW_CONTEXT_CACHE.clear()
        fake_task_inside_dag()

        airflow_dag_inplace_tracking._TRY_GET_AIRFLOW_CONTEXT_CACHE.clear()
        print("hey")
