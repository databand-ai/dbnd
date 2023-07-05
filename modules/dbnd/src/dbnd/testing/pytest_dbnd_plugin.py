# © Copyright Databand.ai, an IBM Company 2022

# we need to import databand module before airflow, otherwise we will not get airflow_bome

import logging
import os

import pytest

from dbnd import dbnd_bootstrap, dbnd_config
from dbnd._core.configuration.environ_config import (
    reset_dbnd_project_config,
    set_dbnd_unit_test_mode,
)
from dbnd._core.context.databand_context import DatabandContext, new_dbnd_context
from dbnd._core.log.config import configure_basic_logging
from dbnd._core.task_build.task_registry import tmp_dbnd_registry
from dbnd._core.task_build.task_signature import TASK_ID_INVALID_CHAR_REGEX


logger = logging.getLogger(__name__)


def pytest_configure(config):
    configure_basic_logging(None)
    set_dbnd_unit_test_mode()


def _run_name_for_test_request(request):
    return TASK_ID_INVALID_CHAR_REGEX.sub("_", request.node.name)


@pytest.fixture
def databand_context_kwargs():
    return {}


@pytest.fixture
def databand_test_context(
    request, tmpdir, databand_context_kwargs
):  # type: (...) -> DatabandContext

    name = f"auto-created by pytest fixture databand_test_context: {os.environ.get('PYTEST_CURRENT_TEST')}"
    databand_context_kwargs.setdefault("name", name)

    dbnd_bootstrap()

    with dbnd_config(
        config_values={
            "local": {
                "root": str(tmpdir.join("local_root"))
            }  # used for legacy dbnd run
        },
        source=name,
    ):
        with new_dbnd_context(**databand_context_kwargs) as t:
            yield t


@pytest.fixture
def databand_clean_project_config_for_every_test():
    reset_dbnd_project_config()
    yield
    reset_dbnd_project_config()


@pytest.fixture
def databand_clean_register_for_every_test():
    with tmp_dbnd_registry() as r:
        yield r


@pytest.fixture
def databand_clean_airflow_vars_for_every_test():
    yield
    for v in [
        "AIRFLOW_CTX_DAG_ID",
        "AIRFLOW_CTX_EXECUTION_DATE",
        "AIRFLOW_CTX_TASK_ID",
        "AIRFLOW_CTX_TRY_NUMBER",
        "AIRFLOW_CTX_UID",
    ]:
        if v in os.environ:
            del os.environ[v]


@pytest.fixture(autouse=True)
def databand_pytest_env(
    databand_clean_project_config_for_every_test,
    databand_test_context,
    databand_clean_register_for_every_test,
    databand_clean_airflow_vars_for_every_test,
):
    """
    all required fixtures to run datband related tests
    add it to your [pytest]usefixtures
    """
    return True
