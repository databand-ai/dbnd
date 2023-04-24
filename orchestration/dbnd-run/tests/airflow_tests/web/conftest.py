# © Copyright Databand.ai, an IBM Company 2022

import logging

import pytest

from dbnd_run.airflow.web.airflow_app_with_versioned_dagbag import (
    create_app as airflow_create_app,
)
from tests.airflow_tests.web.utils import AirflowWebAppCtrl


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def airflow_web_app():
    app, appbuilder = airflow_create_app(testing=True)
    app.config["WTF_CSRF_ENABLED"] = False
    app.web_appbuilder = appbuilder

    return app


@pytest.fixture(scope="session")
def airflow_web_client(airflow_web_app):
    with airflow_web_app.test_client() as c:
        yield c
        logger.info("web client is closed")


@pytest.fixture
def airflow_web_app_ctrl(airflow_web_app, airflow_web_client):
    return AirflowWebAppCtrl(
        app=airflow_web_app,
        appbuilder=airflow_web_app.web_appbuilder,
        client=airflow_web_client,
    )
