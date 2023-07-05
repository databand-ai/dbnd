# © Copyright Databand.ai, an IBM Company 2022

# we need to import databand module before airflow, otherwise we will not get airflow_bome

import logging
import os

import pytest

from dbnd import config, dbnd_bootstrap, dbnd_config
from dbnd._core.configuration.environ_config import reset_dbnd_project_config
from dbnd._core.context.databand_context import DatabandContext, new_dbnd_context
from dbnd._core.context.use_dbnd_run import set_orchestration_mode
from dbnd._core.task_build.task_registry import tmp_dbnd_registry
from dbnd._core.task_build.task_signature import TASK_ID_INVALID_CHAR_REGEX


logger = logging.getLogger(__name__)


def _run_name_for_test_request(request):
    return TASK_ID_INVALID_CHAR_REGEX.sub("_", request.node.name)


@pytest.fixture
def dbnd_clean_project_config():
    reset_dbnd_project_config()

    # dbnd_bootstrap is "sinlge execution",
    # it will run only once per pytest proces.

    dbnd_bootstrap(enable_dbnd_run=True)

    set_orchestration_mode()
    dbnd_config.reset()
    dbnd_config.load_system_configs()

    with tmp_dbnd_registry() as r:
        yield r

    reset_dbnd_project_config()


@pytest.fixture
def dbnd_clean_airflow_vars_for_every_test():
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


@pytest.fixture
def dbnd_config_for_test_run__system(request, tmpdir):
    return {
        "run_info": {"name": _run_name_for_test_request(request)},
        "run": {"heartbeat_interval_s": -1},
        "local": {"root": str(tmpdir.join("local_root"))},
    }


@pytest.fixture
def dbnd_context_kwargs():
    return {}


@pytest.fixture
def dbnd_config_for_test_run__user():
    # USE IT TO OVEERRIDE/ADD EXTRA CONFIG
    return {}


@pytest.fixture
def dbnd_run_test_context(
    dbnd_clean_project_config,
    dbnd_context_kwargs,
    dbnd_config_for_test_run__system,
    dbnd_config_for_test_run__user,
):  # type: (...) -> DatabandContext

    dbnd_context_kwargs.setdefault(
        "name", f"pytest fixture context: {os.environ.get('PYTEST_CURRENT_TEST')}"
    )

    with config(
        dbnd_config_for_test_run__system, source="dbnd_config_for_test_run__system"
    ), config(
        dbnd_config_for_test_run__user, source="dbnd_config_for_test_run__user"
    ), new_dbnd_context(
        **dbnd_context_kwargs
    ) as t:
        yield t


@pytest.fixture
def dbnd_run_pytest_env(dbnd_clean_airflow_vars_for_every_test, dbnd_run_test_context):
    """
    all required fixtures to run datband related tests
    add it to your [pytest]usefixtures
    """
    return True
