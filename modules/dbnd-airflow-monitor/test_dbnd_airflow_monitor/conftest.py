# © Copyright Databand.ai, an IBM Company 2022

import os

import pytest

from pytest import fixture

from dbnd._core.configuration.environ_config import (
    ENV_DBND__ORCHESTRATION__NO_PLUGINS,
    reset_dbnd_project_config,
)
from dbnd._core.utils.basics.environ_utils import set_on
from dbnd.testing.test_config_setter import add_test_configuration

from .mock_airflow_data_fetcher import MockDataFetcher
from .mock_airflow_tracking_service import (
    MockIntegrationManagementService,
    MockReportingService,
    MockTrackingService,
)


home = os.path.abspath(
    os.path.normpath(os.path.join(os.path.dirname(__file__), "home"))
)
os.environ["DBND_HOME"] = home
os.environ["AIRFLOW_HOME"] = home
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ[
    "DBND__WEBSERVER__FERNET_KEY"
] = "-m4wWvVz9cGJPjFSRW1sI9zhTYUwnobQoJZjzXgBsWA="  # pragma: allowlist secret
reset_dbnd_project_config()

# we don't need to load dbnd plugins/modules
set_on(ENV_DBND__ORCHESTRATION__NO_PLUGINS)


def pytest_configure(config):
    add_test_configuration(__file__)


@fixture
def unittests_db():
    return "fetch-unittests.db"


@fixture
def empty_db():
    return "empty-unittests.db"


@fixture
def incomplete_data_db():
    return "incomplete-unittests.db"


@pytest.fixture
def mock_tracking_service() -> MockTrackingService:
    yield MockTrackingService()


@pytest.fixture
def mock_data_fetcher() -> MockDataFetcher:
    yield MockDataFetcher()


@pytest.fixture
def mock_integration_management_service() -> MockIntegrationManagementService:
    yield MockIntegrationManagementService()


@pytest.fixture
def mock_reporting_service() -> MockReportingService:
    yield MockReportingService("airflow")
