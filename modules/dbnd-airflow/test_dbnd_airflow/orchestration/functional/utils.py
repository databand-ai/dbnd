import logging
import os

from datetime import datetime

from airflow import DAG, settings
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.models import XCom
from airflow.utils.dates import days_ago

from dbnd import relative_path
from dbnd._core.utils.project.project_fs import abs_join
from dbnd._core.utils.timezone import utcnow
from targets import target


logger = logging.getLogger(__name__)
_lib_dbnd_airflow_operator_test = relative_path(__file__, "../..", "..", "..")


def get_executor_for_test():
    try:
        from dbnd_airflow.executors.simple_executor import InProcessExecutor

        return InProcessExecutor()
    except Exception:
        return SequentialExecutor()


def dbnd_airflow_operator_test_path(*path):
    return abs_join(_lib_dbnd_airflow_operator_test, *path)


def dbnd_airflow_operator_home_path(*path):
    return dbnd_airflow_operator_test_path("home", *path)


def run_and_get(dag, task_id, execution_date=None):
    execution_date = execution_date or utcnow()
    try:
        _run_dag(dag, execution_date=execution_date)
    except Exception as ex:
        logger.exception("Failed to run %s %s %s", dag.dag_id, task_id, execution_date)

        from dbnd_airflow.compat.airflow_multi_version_shim import (
            get_airflow_conf_log_folder,
        )

        iso = execution_date.isoformat()
        log = os.path.expanduser(get_airflow_conf_log_folder())
        log_path = os.path.join(log, dag.dag_id, task_id, iso)
        logger.info("Check logs at %s", log_path)
        raise

    return _get_result(dag, task_id, execution_date=execution_date)


def _run_dag(dag, execution_date):
    # type: (DAG, datetime) -> None
    current_dag_folder = settings.conf.get("core", "dags_folder")
    try:
        settings.conf.set("core", "dags_folder", dag.fileloc)
        dag.run(
            start_date=execution_date,
            end_date=execution_date,
            executor=get_executor_for_test(),
        )
    finally:
        settings.conf.set("core", "dags_folder", current_dag_folder)


def _get_result(dag, task_id, execution_date=None):
    execution_date = execution_date or days_ago(0)
    result = XCom.get_one(
        execution_date=execution_date, task_id=task_id, dag_id=dag.dag_id
    )
    assert result
    return result


def read_xcom_result_value(xcom_result, key="result"):
    result_path = xcom_result[key]
    return target(result_path).read()
