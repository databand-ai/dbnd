import os

import mock

from pytest import fixture

from dbnd import log_metric, task
from dbnd._core.context.databand_context import DatabandContext
from dbnd._core.task_run.task_run_logging import TaskRunLogManager
from dbnd._core.task_run.task_run_tracker import TaskRunTracker


def raise_exception():
    x = 2 / 0


def fake_context_on_enter(self):
    raise_exception()


def fake_save_log_preview(self, log_body):
    raise_exception()


def fake_save_task_run_log(self, log_preview):
    raise_exception()


patch_dict = {
    "AIRFLOW_CTX_DAG_ID": "simple_dag",
    "AIRFLOW_CTX_TASK_ID": "simple_dag_task",
    "AIRFLOW_CTX_EXECUTION_DATE": "2020-04-06T14:25:00",
}


@fixture
def set_env():
    # patch.dict updates only the given variables (without touching others)
    with mock.patch.dict(os.environ, patch_dict):
        yield


class MyClass(object):
    @staticmethod
    def some_function(num):
        return sum(i for i in range(num))


@task
def some_task():
    log_metric("result", MyClass.some_function(5))


def try_run_function():
    with mock.patch.object(MyClass, "some_function", autospec=True) as func:
        for _ in range(5):
            some_task()
        assert func.call_count == 5


def test_context_creation_fail(set_env):
    with mock.patch.object(DatabandContext, "_on_enter", fake_context_on_enter):
        try_run_function()


def test_webserver_connection_fail(set_env):
    with mock.patch.object(TaskRunTracker, "save_task_run_log", fake_save_task_run_log):
        try_run_function()


def test_log_file_write_fail(set_env):
    with mock.patch.object(
        TaskRunLogManager, "save_log_preview", fake_save_log_preview
    ):
        try_run_function()
