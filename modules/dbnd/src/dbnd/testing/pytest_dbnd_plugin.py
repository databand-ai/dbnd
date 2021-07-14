# we need to import databand module before airflow, otherwise we will not get airflow_bome

import cProfile
import logging
import os
import random
import time

from datetime import datetime

import pytest

from _pytest.fixtures import fixture

from dbnd import config
from dbnd._core.configuration.environ_config import (
    reset_dbnd_project_config,
    set_dbnd_unit_test_mode,
)
from dbnd._core.context.bootstrap import dbnd_bootstrap
from dbnd._core.context.databand_context import DatabandContext, new_dbnd_context
from dbnd._core.log.config import configure_basic_logging
from dbnd._core.task_build.task_registry import tmp_dbnd_registry
from dbnd._core.task_build.task_signature import TASK_ID_INVALID_CHAR_REGEX


logger = logging.getLogger(__name__)


def pytest_configure(config):
    configure_basic_logging(None)
    set_dbnd_unit_test_mode()
    dbnd_bootstrap()


def pytest_addoption(parser):
    # group = parser.getgroup("dnbd")
    # group._addoption(
    #     "--reuse-db",
    #     action="store_true",
    #     dest="reuse_db",
    #     default=False,
    #     help="Re-use the testing DB if it already exists, "
    #     "and do not remove it when the test finishes.",
    # )
    pass


def pytest_load_initial_conftests(early_config, parser, args):
    early_config.addinivalue_line("markers", "dbnd(: Mark the test as using dbnd")

    early_config._dbnd_report_header = None
    early_config._dbnd_report_header = "Databand: %s" % ("")


def pytest_report_header(config):
    if hasattr(config, "_dbnd_report_header") and config._dbnd_report_header:
        return [config._dbnd_report_header]


def _run_name_for_test_request(request):
    return TASK_ID_INVALID_CHAR_REGEX.sub("_", request.node.name)


@pytest.fixture(scope="session")
def databand_test_db():
    # init_test_db()
    # we are going to initialize db at databand context
    pass


@pytest.fixture
def databand_context_kwargs():
    return {}


@pytest.fixture(scope="session")
def databand_config():
    config.load_system_configs()
    return config


@pytest.fixture
def databand_test_context(
    request, tmpdir, databand_context_kwargs, databand_config
):  # type: (...) -> DatabandContext

    test_config = {
        "run": {
            "name": _run_name_for_test_request(request),
            "heartbeat_interval_s": -1,
        },
        "local": {"root": str(tmpdir.join("local_root"))},
    }
    databand_context_kwargs.setdefault("name", "databand_test_context")
    with config(test_config, source="databand_test_context"), new_dbnd_context(
        **databand_context_kwargs
    ) as t:
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


@pytest.fixture(autouse=True)
def databand_pytest_env(
    databand_clean_project_config_for_every_test,
    databand_test_db,
    databand_test_context,
    databand_clean_register_for_every_test,
):
    """
    all required fixtures to run datband related tests
    add it to your [pytest]usefixtures
    """
    return True


@pytest.fixture()
def mock_target_fs():
    from targets.fs.mock import MockFileSystem

    MockFileSystem.instance.clear()
    yield MockFileSystem.instance
    MockFileSystem.instance.clear()


@fixture
def cProfile_benchmark():
    perf_stats_dir = "/tmp/benchmark/"

    def _benchmark(fn, *args, **kwargs):
        unique_id = datetime.now().strftime(
            "%Y%m%d-%H%M%S-" + str(random.randint(0, 10000))
        )
        # with perf_trace(os.path.join(perf_stats_dir, "p1_%s.stats" % (unique_id))):
        file = os.path.join(perf_stats_dir, "benchmark_%s.stats" % (unique_id))
        start = time.time()
        pr = cProfile.Profile()
        pr.enable()

        result = fn(*args, **kwargs)
        done = time.time()
        pr.disable()

        file = os.path.abspath(file)
        if not os.path.exists(os.path.dirname(file)):
            os.makedirs(os.path.dirname(file))

        pr.dump_stats(file)
        logging.warning("Performance report saved at %s", file)
        elapsed = done - start
        logger.warning("Time Elapsed for %s: %s", fn, elapsed)
        return result

    return _benchmark
