# inline conftest
import datetime
import uuid

from pytest import fixture

from dbnd import config


pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
]


@fixture
def hdfs_path():
    return "hdfs://{}_{}_{}".format(
        str(config.get("integration_tests", "hdfs_folder")),
        datetime.datetime.today().strftime("%Y-%m-%d"),
        str(uuid.uuid4()),
    )
