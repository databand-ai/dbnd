# © Copyright Databand.ai, an IBM Company 2022
import pytest


# inline conftest

pytest_plugins = [
    "dbnd_run.testing.pytest_dbnd_run_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]


# MAIN FIXTURE, USED BY ALL TESTS ###


@pytest.fixture(autouse=True)
def dbnd_env_per_test(dbnd_run_pytest_env):
    yield dbnd_run_pytest_env
