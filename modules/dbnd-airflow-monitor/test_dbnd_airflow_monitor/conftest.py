from __future__ import print_function

import os

import pytest

from pytest import fixture

from dbnd._core.configuration.environ_config import reset_dbnd_project_config

from .mock_airflow_data_fetcher import MockDataFetcher
from .mock_airflow_tracking_service import MockServersConfigService, MockTrackingService


home = os.path.abspath(
    os.path.normpath(os.path.join(os.path.dirname(__file__), "home"))
)
os.environ["DBND_HOME"] = home
os.environ["AIRFLOW_HOME"] = home
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
reset_dbnd_project_config()


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
def mock_server_config_service():
    yield MockServersConfigService()


@pytest.fixture
def mock_tracking_service():
    yield MockTrackingService()


@pytest.fixture
def mock_data_fetcher():
    yield MockDataFetcher()
