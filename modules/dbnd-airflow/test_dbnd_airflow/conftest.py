from __future__ import print_function

# inline conftest
import logging
import os
import sys

import pytest


# should be before any databand import!
home = os.path.abspath(
    os.path.normpath(os.path.join(os.path.dirname(__file__), "airflow_home"))
)  # isort:skip
os.environ["DBND_HOME"] = home  # isort:skip
os.environ["AIRFLOW_HOME"] = home  # isort:skip
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"  # isort:skip


# make test_dbnd available
from dbnd.testing.helpers import dbnd_module_path  # isort:skip

# import dbnd should be first!
from dbnd_airflow.testing.unittest_env import setup_unittest_airflow  # isort:skip

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
    yield databand_pytest_env


@pytest.fixture
def af_session():
    from airflow.utils.db import create_session

    with create_session() as session:
        yield session
