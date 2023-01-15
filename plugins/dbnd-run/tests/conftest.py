# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

# inline conftest
import logging
import os
import sys

import pytest


# should be before any databand import!
dbnd_system_home = os.path.abspath(
    os.path.normpath(os.path.join(os.path.dirname(__file__), ".dbnd"))
)  # isort:skip
airflow_home = os.path.abspath(
    os.path.normpath(os.path.join(os.path.dirname(__file__), "airflow", "airflow_home"))
)  # isort:skip
os.environ["DBND_SYSTEM"] = dbnd_system_home  # isort:skip
os.environ["AIRFLOW_HOME"] = airflow_home  # isort:skip
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"  # isort:skip


# make test_dbnd available
from dbnd.testing.helpers import dbnd_module_path  # isort:skip

# import dbnd should be first!
from dbnd_run.airflow.testing.unittest_env import setup_unittest_airflow  # isort:skip

sys.path.append(dbnd_module_path())

logger = logging.getLogger(__name__)

pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]


@pytest.fixture(autouse=True, scope="session")
def dbnd_airflow_unittest_setup():
    setup_unittest_airflow()


@pytest.fixture(autouse=True)
def dbnd_env_per_test(databand_pytest_env):
    _clean_af_env()
    yield databand_pytest_env
    _clean_af_env()


def _clean_af_env():
    for env_var in [
        "AIRFLOW_CTX_DAG_ID",
        "AIRFLOW_CTX_EXECUTION_DATE",
        "AIRFLOW_CTX_TASK_ID",
        "AIRFLOW_CTX_TRY_NUMBER",
        "AIRFLOW_CTX_UID",
    ]:
        if env_var in os.environ:
            del os.environ[env_var]


@pytest.fixture
def af_session():
    from airflow.utils.db import create_session

    with create_session() as session:
        yield session
