# © Copyright Databand.ai, an IBM Company 2022

# inline conftest
import datetime
import uuid

from pytest import fixture

from dbnd import dbnd_bootstrap, dbnd_config
from dbnd.testing.test_config_setter import add_test_configuration


pytest_plugins = [
    "dbnd.orchestration.testing.pytest_dbnd_run_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]


def pytest_configure(config):
    dbnd_bootstrap(enable_dbnd_run=True)
    add_test_configuration(__file__)


@fixture
def hdfs_path():
    return "hdfs://{}_{}_{}".format(
        str(dbnd_config.get("integration_tests", "hdfs_folder")),
        datetime.datetime.today().strftime("%Y-%m-%d"),
        str(uuid.uuid4()),
    )
