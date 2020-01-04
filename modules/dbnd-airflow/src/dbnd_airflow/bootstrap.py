import os
import sys

from dbnd_airflow.airflow_override import monkeypatch_airflow


def _register_sqlachemy_local_dag_job():
    from dbnd_airflow.scheduler.single_dag_run_job import SingleDagRunJob

    # we also need to load this object, so webserver will see our LocalDagJobs objectsimpo
    return SingleDagRunJob


def _fix_sys_path():
    # airflow append DAG and PLUGIN folders to sys.path
    # they are coming from ConfigParser, python2 backport create newbytes values instead of str
    # any module that uses Path will fail on that
    sys.path = [str(p) if type(p) != str else p for p in sys.path]


def airflow_bootstrap():
    monkeypatch_airflow()
    from dbnd_airflow_windows.airflow_windows_support import (
        enable_airflow_windows_support,
    )

    if os.name == "nt":
        enable_airflow_windows_support()
    _fix_sys_path()
    _register_sqlachemy_local_dag_job()
