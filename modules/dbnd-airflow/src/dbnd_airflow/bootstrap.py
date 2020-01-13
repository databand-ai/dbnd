import logging
import os
import sys


def _register_sqlachemy_local_dag_job():
    from dbnd_airflow.scheduler.single_dag_run_job import SingleDagRunJob

    # we also need to load this object, so webserver will see our LocalDagJobs objectsimpo
    return SingleDagRunJob


def _fix_sys_path():
    # airflow append DAG and PLUGIN folders to sys.path
    # they are coming from ConfigParser, python2 backport create newbytes values instead of str
    # any module that uses Path will fail on that
    sys.path = [str(p) if type(p) != str else p for p in sys.path]


def set_airflow_sql_conn_from_dbnd_config():
    logging.debug("updating airflow config from dbnd config")
    from dbnd._core.configuration.dbnd_config import config as dbnd_config

    sql_alchemy_conn = dbnd_config.get("airflow", "sql_alchemy_conn")
    if sql_alchemy_conn == "dbnd":
        logging.debug("updating airflow sql from dbnd core.sql_alchemy_conn")
        sql_alchemy_conn = dbnd_config.get("core", "sql_alchemy_conn")

    if sql_alchemy_conn and "AIRFLOW__CORE__SQL_ALCHEMY_CONN" not in os.environ:
        os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = sql_alchemy_conn

    fernet_key = dbnd_config.get("airflow", "fernet_key")
    if fernet_key == "dbnd":
        fernet_key = dbnd_config.get("core", "fernet_key")
    if fernet_key and "AIRFLOW__CORE__FERNET_KEY" not in os.environ:
        os.environ["AIRFLOW__CORE__FERNET_KEY"] = fernet_key


_airflow_bootstrap_applied = False


def airflow_bootstrap():
    global _airflow_bootstrap_applied
    if _airflow_bootstrap_applied:
        return
    set_airflow_sql_conn_from_dbnd_config()

    from dbnd_airflow.airflow_override import monkeypatch_airflow

    monkeypatch_airflow()
    from dbnd_airflow_windows.airflow_windows_support import (
        enable_airflow_windows_support,
    )

    if os.name == "nt":
        enable_airflow_windows_support()
    _fix_sys_path()
    _register_sqlachemy_local_dag_job()
    _airflow_bootstrap_applied = True
