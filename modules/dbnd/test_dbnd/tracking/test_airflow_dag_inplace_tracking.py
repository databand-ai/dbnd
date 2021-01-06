import pytest

from mock import Mock

from dbnd._core.constants import UpdateSource
from dbnd._core.tracking.airflow_dag_inplace_tracking import (
    AirflowOperatorRuntimeTask,
    AirflowTaskContext,
    build_run_time_airflow_task,
)


def af_context_w_context():
    task_instance = Mock()
    task = Mock()
    task.template_fields = ["a", "b", "c"]
    task.a, task.b, task.c = 1, 1, 2

    task_instance.task = task
    task_instance.dag_id = "test_dag"
    task_instance.execution_date = "1970-01-01T00:00:00.000000+00:00"
    task_instance.task_id = "test_task"
    task_instance.try_number = 6
    task_instance.log_filepath = "mylog.log"

    context = {"task_instance": task_instance}
    return AirflowTaskContext(
        dag_id="test_dag",
        execution_date="1970-01-01T00:00:00.571846+00:00",
        task_id="test_task",
        try_number=6,
        context=context,
    )


def af_context_wo_context():
    return AirflowTaskContext(
        dag_id="test_dag",
        execution_date="1970-01-01T00:00:00.571846+00:00",
        task_id="test_task",
        try_number=6,
    )


@pytest.mark.parametrize(
    "context, expected_name, fields",
    [
        (af_context_w_context(), "test_task__execute", ["a", "b", "c"]),
        (af_context_wo_context(), ".py", []),
    ],
)
def test_build_run_time_airflow_task(context, expected_name, fields):
    root_task, job_name, source = build_run_time_airflow_task(context)
    assert job_name == "test_dag.test_task"
    assert source == UpdateSource.airflow_tracking
    assert expected_name in root_task.task_name
    for field in fields:
        assert hasattr(root_task, field)
    assert isinstance(root_task, AirflowOperatorRuntimeTask)
