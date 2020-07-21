# inline conftest
import datetime
import uuid

from pytest import fixture

from dbnd import dbnd_config, relative_path


pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
]

dbnd_config.set_from_config_file(relative_path(__file__, "databand-test.cfg"))


@fixture
def hdfs_path():
    return "hdfs://{}_{}_{}".format(
        str(dbnd_config.get("integration_tests", "hdfs_folder")),
        datetime.datetime.today().strftime("%Y-%m-%d"),
        str(uuid.uuid4()),
    )
