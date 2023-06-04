# © Copyright Databand.ai, an IBM Company 2022
from unittest.mock import patch
from uuid import UUID

from mock import Mock

from dbnd import log_metric
from dbnd._core.constants import UpdateSource
from dbnd._core.task_build.task_source_code import NO_SOURCE_CODE
from dbnd._core.tracking.airflow_dag_inplace_tracking import (
    AirflowTaskContext,
    build_run_time_airflow_task,
    get_task_family_for_inline_script,
    get_task_run_uid_for_inline_script,
)
from dbnd._core.tracking.script_tracking_manager import (
    dbnd_airflow_tracking_start,
    dbnd_tracking,
    dbnd_tracking_stop,
)
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
    root_task, job_name, source, run_uid = build_run_time_airflow_task(
        context, "some_name"
    )

    assert job_name == "test_dag"
    assert source == UpdateSource.airflow_tracking
    assert "test_task" in root_task.task_name
    assert root_task.task_family == "test_task"

    for field in ["a", "b", "c"]:
        assert root_task.task_params.get_value(field)


@patch("dbnd._core.tracking.airflow_dag_inplace_tracking.is_instance_by_class_name")
@patch("dbnd._core.tracking.airflow_dag_inplace_tracking.TaskSourceCode.from_callable")
def test_build_run_time_airflow_task_with_functools_partial(
    mock_task_source_code, mock_is_instance_by_class_name
):
    mock_is_instance_by_class_name.return_value = True
    mock_task_source_code.return_value = NO_SOURCE_CODE
    context = af_context_w_context()
    context.python_callable = "st"
    root_task, job_name, source, run_uid = build_run_time_airflow_task(
        context, "some_name"
    )

    assert job_name == "test_dag"
    assert source == UpdateSource.airflow_tracking
    assert "test_task" in root_task.task_name
    assert root_task.task_family == "test_task"

    for field in ["a", "b", "c"]:
        assert root_task.task_params.get_value(field)


def test_build_run_time_airflow_task_without_context():
    context = af_context_wo_context()
    root_task, job_name, source, run_uid = build_run_time_airflow_task(
        context, "special_name"
    )

    assert job_name == "test_dag"
    assert source == UpdateSource.airflow_tracking
    assert root_task.task_name == "test_task_special_name"
    assert root_task.task_family == "test_task_special_name"


def test_script_tracking_with_airflow_context_from_env():
    with env(
        AIRFLOW_CTX_DAG_ID="test_dag",
        AIRFLOW_CTX_EXECUTION_DATE="1970-01-01T00:00:00.571846+00:00",
        AIRFLOW_CTX_TASK_ID="test_task",
        AIRFLOW_CTX_TRY_NUMBER="6",
    ), dbnd_tracking(job_name="boom") as task_run:
        assert task_run.task.task_name == "test_task_boom"


def test_script_tracking():
    with dbnd_tracking(job_name="boom") as task_run:
        assert task_run.task.task_name == "boom"


def test_tracking():
    af_context = af_context_w_context()
    dbnd_airflow_tracking_start(airflow_context=af_context)
    log_metric("test", "test_value")
    dbnd_tracking_stop()


def test_get_task_family_for_inline_script():
    actual = get_task_family_for_inline_script("sample_task", "root_task")
    assert actual == "sample_task_root_task"


def test_get_task_run_uid_for_inline_script():
    tracking_env = {
        "AIRFLOW_CTX_TASK_ID": "potholes_report",
        "DBND_ROOT_RUN_UID": "15683ffd-9bd8-5732-8288-4a48aea0db2b",
        "AIRFLOW_CTX_DAG_ID": "service311_ingest_data",
        "AIRFLOW_CTX_TRY_NUMBER": "1",
    }
    script_name = "potholes_report.py"
    task_id, task_run_uid, task_run_attempt_uid = get_task_run_uid_for_inline_script(
        tracking_env, script_name
    )
    assert task_id == "potholes_report_potholes_report.py"
    assert task_run_uid == UUID("f79ff618-a456-5835-9bc5-c7536e174157")
    assert task_run_attempt_uid == UUID("0396fa7a-7e95-510f-a6b4-f198accb8ae5")
