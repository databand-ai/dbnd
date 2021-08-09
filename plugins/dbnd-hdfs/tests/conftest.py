# inline conftest
import datetime
import uuid

from pytest import fixture

from dbnd import dbnd_config
from dbnd.testing.test_config_setter import add_test_configuration


pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
    "dbnd.testing.pytest_dbnd_home_plugin",
]


def pytest_configure(config):
    add_test_configuration(__file__)


@fixture
def hdfs_path():
    return "hdfs://{}_{}_{}".format(
        str(dbnd_config.get("integration_tests", "hdfs_folder")),
        datetime.datetime.today().strftime("%Y-%m-%d"),
        str(uuid.uuid4()),
    )
