import os
import sys

from dbnd_airflow.airflow_override import monkeypatch_airflow


def _set_kerberos_env():
    from airflow import configuration as airflow_configuration

    if airflow_configuration.conf.get("core", "security") == "kerberos":
        os.environ["KRB5CCNAME"] = airflow_configuration.conf.get("kerberos", "ccache")
        os.environ["KRB5_KTNAME"] = airflow_configuration.conf.get("kerberos", "keytab")


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
    _fix_sys_path()
    _set_kerberos_env()
    _register_sqlachemy_local_dag_job()

    from dbnd import dbnd_config

    if dbnd_config.getboolean("airflow", "track_airflow_dag"):
        from dbnd_airflow.airflow_override import enable_airflow_tracking

        enable_airflow_tracking()
