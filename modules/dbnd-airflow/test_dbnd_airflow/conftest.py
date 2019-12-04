# inline conftest
import logging

import pytest

from dbnd_airflow.web.airflow_app import create_app
from test_dbnd_airflow.utils import WebAppCtrl


pytest_plugins = [
    "dbnd.testing.pytest_dbnd_plugin",
    "dbnd.testing.pytest_dbnd_markers_plugin",
]
logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def dbnd_env_per_test(databand_pytest_env):
    yield databand_pytest_env


@pytest.fixture
def af_session():
    from airflow.utils.db import create_session

    with create_session() as session:
        yield session


@pytest.fixture(scope="session")
def web_app():
    app, appbuilder = create_app(testing=True)
    app.config["WTF_CSRF_ENABLED"] = False
    app.web_appbuilder = appbuilder
    return app


@pytest.fixture(scope="session")
def web_client(web_app):
    with web_app.test_client() as c:
        yield c
        logger.info("web client is closed")


@pytest.fixture
def web_app_ctrl(web_app, web_client):
    return WebAppCtrl(app=web_app, appbuilder=web_app.web_appbuilder, client=web_client)
