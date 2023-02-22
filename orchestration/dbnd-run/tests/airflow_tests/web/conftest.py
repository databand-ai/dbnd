# © Copyright Databand.ai, an IBM Company 2022

import logging

import pytest

from dbnd_run.airflow.web.airflow_app_with_versioned_dagbag import create_app
from tests.airflow_tests.web.utils import WebAppCtrl


logger = logging.getLogger(__name__)


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
