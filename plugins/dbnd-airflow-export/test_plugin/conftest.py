from __future__ import print_function

import os

import attr
import mock

from pytest import fixture


@fixture(scope="session")
def airflow_home():
    home_path = os.path.abspath(
        os.path.normpath(os.path.join(os.path.dirname(__file__), "airflow"))
    )
    with mock.patch.dict(os.environ, {"AIRFLOW_HOME": home_path}):
        yield home_path


@fixture(scope="session")
def airflow_sqlalchemy_conn(airflow_home):
    connection_string = "sqlite:///" + os.path.join(airflow_home, "airflow.db")
    return connection_string


@fixture(scope="session")
def airflow_dag_folder(airflow_home):
    return os.path.join(airflow_home, "dags")


@fixture(scope="session")
def airflow_dagbag(airflow_sqlalchemy_conn, airflow_dag_folder):
    from airflow import models, conf
    from airflow.settings import STORE_SERIALIZED_DAGS

    conf.set("core", "sql_alchemy_conn", value=airflow_sqlalchemy_conn)
    dagbag = models.DagBag(
        airflow_dag_folder,
        include_examples=True,
        store_serialized_dags=STORE_SERIALIZED_DAGS,
    )
    return dagbag


@fixture(autouse=True, scope="session")
def airflow_init_db(airflow_sqlalchemy_conn):
    from airflow.bin.cli import initdb

    initdb([])


@attr.s
class YesObject(object):
    yes = attr.ib()  # type: bool


@fixture(autouse=True, scope="function")
def airflow_reset_db(airflow_sqlalchemy_conn):
    from airflow.bin.cli import resetdb
    from test_plugin.db_data_generator import set_dag_is_paused

    resetdb(YesObject(yes=True))

    set_dag_is_paused(is_paused=False)