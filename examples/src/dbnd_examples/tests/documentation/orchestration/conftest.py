# © Copyright Databand.ai, an IBM Company 2022
import pytest

from dbnd._core.context.use_dbnd_run import disable_airflow_package


disable_airflow_package()

# MAIN FIXTURE, USED BY ALL TESTS ###


@pytest.fixture(autouse=True)
def dbnd_env_per_test(dbnd_run_pytest_env):
    yield dbnd_run_pytest_env
