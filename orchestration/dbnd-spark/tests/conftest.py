# © Copyright Databand.ai, an IBM Company 2022

import pytest

from dbnd import dbnd_bootstrap
from dbnd.testing.test_config_setter import add_test_configuration


pytest_plugins = [
    "dbnd.orchestration.testing.pytest_dbnd_run_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]

skip_require_java_build = pytest.mark.skip("Requires JAVA project")

### MAIN FIXTURE, USED BY ALL TESTS ###
@pytest.fixture(autouse=True)
def dbnd_env_per_test(dbnd_run_pytest_env):
    add_test_configuration(__file__)

    yield dbnd_run_pytest_env


def pytest_configure(config):

    dbnd_bootstrap(enable_dbnd_run=True)
    add_test_configuration(__file__)
