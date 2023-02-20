# © Copyright Databand.ai, an IBM Company 2022

from mock import MagicMock

from dbnd import Task
from dbnd._core.context.databand_context import DatabandContext
from dbnd._core.errors import DatabandError, DatabandRuntimeError
from dbnd._core.settings import DatabandSettings
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.task_run.task_run_error import TaskRunError


def test_as_error_info():
    try:
        raise DatabandRuntimeError(
            "DATABAND_ACCESS_TOKEN=eyJ0eXAiOiJKV1QGciOiJIUzI1NiJ9"  # pragma: allowlist secret
        )
    except DatabandError as ex:
        task_run = MagicMock(TaskRun)
        task_run.airflow_context = None
        task = MagicMock(Task)
        context = DatabandContext()
        task.settings = DatabandSettings(context)
        task_run.task = task

        error = TaskRunError.build_from_ex(ex, task_run)
        out = error.as_error_info()
        assert "DATABAND_ACCESS_TOKEN=***" in out.user_code_traceback
        assert "eyJ0eXAiOiJKV1QGciOiJIUzI1NiJ9" not in out.user_code_traceback
