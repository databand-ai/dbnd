from __future__ import print_function

import os

import pytest

from pytest import fixture

from airflow_monitor.data_fetcher import decorate_fetcher
from airflow_monitor.tracking_service import (
    decorate_configuration_service,
    decorate_tracking_service,
)
from dbnd._core.configuration.environ_config import (
    get_dbnd_project_config,
    reset_dbnd_project_config,
)

from .mock_airflow_data_fetcher import MockDataFetcher
from .mock_airflow_tracking_service import MockServersConfigService, MockTrackingService


home = os.path.abspath(
    os.path.normpath(os.path.join(os.path.dirname(__file__), "home"))
)
os.environ["DBND_HOME"] = home
os.environ["AIRFLOW_HOME"] = home
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
reset_dbnd_project_config()

# we don't need to load dbnd plugins/modules
get_dbnd_project_config().is_no_modules = True


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
def mock_server_config_service() -> MockServersConfigService:
    yield decorate_configuration_service(MockServersConfigService())


@pytest.fixture
def mock_tracking_service() -> MockTrackingService:
    yield decorate_tracking_service(MockTrackingService(), "mock_ts")


@pytest.fixture
def mock_data_fetcher() -> MockDataFetcher:
    yield decorate_fetcher(MockDataFetcher(), "mock_fetcher")
