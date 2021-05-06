from mock import Mock

from dbnd._core.constants import UpdateSource
from dbnd._core.tracking.airflow_dag_inplace_tracking import (
    AIRFLOW_TRACKING_ROOT_TASK_NAME,
    AirflowTaskContext,
    build_run_time_airflow_task,
)
from dbnd._core.tracking.script_tracking_manager import dbnd_tracking
from dbnd._core.utils.basics.environ_utils import env


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


def test_build_run_time_airflow_task_with_context():
    context = af_context_w_context()
    root_task, job_name, source = build_run_time_airflow_task(context, "some_name")

    assert job_name == "test_dag.test_task"
    assert source == UpdateSource.airflow_tracking
    assert "test_task__execute" in root_task.task_name
    assert root_task.task_family == AIRFLOW_TRACKING_ROOT_TASK_NAME

    for field in ["a", "b", "c"]:
        assert root_task.task_params.get_value(field)


def test_build_run_time_airflow_task_without_context():
    context = af_context_wo_context()
    root_task, job_name, source = build_run_time_airflow_task(context, "special_name")

    assert job_name == "test_dag.test_task"
    assert source == UpdateSource.airflow_tracking
    assert root_task.task_name == "special_name"
    assert root_task.task_family == AIRFLOW_TRACKING_ROOT_TASK_NAME


def test_script_tracking_with_airflow_context_from_env():
    with env(
        AIRFLOW_CTX_DAG_ID="test_dag",
        AIRFLOW_CTX_EXECUTION_DATE="1970-01-01T00:00:00.571846+00:00",
        AIRFLOW_CTX_TASK_ID="test_task",
        AIRFLOW_CTX_TRY_NUMBER="6",
    ), dbnd_tracking(name="boom") as task_run:
        assert task_run.task.task_name == "boom"


def test_script_tracking():
    with dbnd_tracking(name="boom") as task_run:
        assert task_run.task.task_name == "boom"
