from __future__ import print_function

import logging
import os

import pytest

from dbnd._core.plugin.dbnd_plugins import pm


home = os.path.abspath(
    os.path.normpath(os.path.join(os.path.dirname(__file__), "airflow_home"))
)
os.environ["DBND_HOME"] = home
os.environ["AIRFLOW_HOME"] = home
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"

logger = logging.getLogger(__name__)

# import dbnd should be first!


pm.set_blocked("dbnd-airflow")


@pytest.fixture(autouse=True, scope="session")
def setup_airflow_test_env():
    from airflow import configuration as airflow_configuration
    from airflow.utils import db

    # we can't call load_test_config, as it override airflow.cfg
    # we want to keep it as base

    sql_alchemy_conn = airflow_configuration.get("core", "sql_alchemy_conn")
    if sql_alchemy_conn.find("unittest.db") == -1:
        raise Exception(
            "You should set SQL_ALCHEMY_CONN to sqlite:///.../unittest.db for tests! Got '%s' instead!"
            % sql_alchemy_conn
        )

    db.initdb(rbac=True)
