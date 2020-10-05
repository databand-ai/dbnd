import logging
import os

from dbnd._core.configuration.environ_config import reset_dbnd_project_config


logger = logging.getLogger(__name__)


TEST_DBND_HOME = "/tmp/test-dbnd"


def pytest_configure(config):
    if not os.path.exists(TEST_DBND_HOME):
        os.mkdir(TEST_DBND_HOME)
    os.environ["DBND_HOME"] = TEST_DBND_HOME
    reset_dbnd_project_config()
