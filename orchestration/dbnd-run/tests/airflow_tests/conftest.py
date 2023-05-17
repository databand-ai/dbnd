# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

# inline conftest
import os

import pytest


from dbnd_run.airflow.testing.unittest_env import setup_unittest_airflow  # isort:skip


@pytest.fixture(autouse=True, scope="module")
def dbnd_airflow_unittest_setup():
    setup_unittest_airflow()


@pytest.fixture(autouse=True, scope="session")
def dbnd_airflow_clean():
    _clean_af_env()
    yield
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
