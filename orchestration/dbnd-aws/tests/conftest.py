# © Copyright Databand.ai, an IBM Company 2022

# inline conftest

import datetime
import uuid

from pytest import fixture

from dbnd import dbnd_config
from dbnd._core.configuration.environ_config import set_orchestration_mode
from dbnd.testing.test_config_setter import add_test_configuration


# we need to dbnd module before airflow, otherwise we will not get airflow_bome
pytest_plugins = [
    "dbnd.testing.pytest_dbnd_home_plugin",
    "dbnd.orchestration.testing.pytest_dbnd_run_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
]


def pytest_configure(config):
    set_orchestration_mode()
    add_test_configuration(__file__)


@fixture
def s3_path():

    return "s3://{}/{}/{}".format(
        str(dbnd_config.get("aws_tests", "bucket_name")),
        datetime.datetime.today().strftime("%Y-%m-%d"),
        str(uuid.uuid4()),
    )
