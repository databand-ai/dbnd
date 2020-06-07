# inline conftest
import logging

import pytest

from dbnd_airflow.testing.unittest_env import setup_unittest_airflow


pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
]
logger = logging.getLogger(__name__)


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
