# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import os

import attr

from pytest import fixture


@fixture(scope="session")
def airflow_home():
    yield os.environ["AIRFLOW_HOME"]


@fixture(scope="session")
def airflow_sqlalchemy_conn(airflow_home):
    connection_string = "sqlite:///" + os.path.join(airflow_home, "airflow.db")
    return connection_string


@fixture(scope="session")
def airflow_dag_folder(airflow_home):
    return os.path.join(airflow_home, "dags")


@fixture(autouse=True, scope="session")
def airflow_init_db(airflow_sqlalchemy_conn):
    try:
        from airflow.bin.cli import initdb
    except ImportError:
        from airflow.cli.commands.db_command import initdb

    initdb([])


@attr.s
class YesObject(object):
    yes = attr.ib()  # type: bool


@fixture(autouse=True, scope="function")
def airflow_reset_db(airflow_sqlalchemy_conn):
    try:
        from airflow.bin.cli import resetdb
    except ImportError:
        from airflow.cli.commands.db_command import resetdb

    from test_dbnd_airflow.export_plugin.db_data_generator import set_dag_is_paused

    resetdb(YesObject(yes=True))

    set_dag_is_paused(is_paused=False)
