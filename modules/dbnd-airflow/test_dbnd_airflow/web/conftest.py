import logging

import pytest

from dbnd_airflow.web.airflow_app import create_app
from test_dbnd_airflow.web.utils import WebAppCtrl


logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def web_app():
    app, appbuilder = create_app(testing=True)
    app.config["WTF_CSRF_ENABLED"] = False
    app.web_appbuilder = appbuilder

    # this import might causes circular dependency if placed above
    from dbnd_airflow.airflow_override.dbnd_aiflow_webserver import (
        use_databand_airflow_dagbag,
    )

    use_databand_airflow_dagbag()
    return app


@pytest.fixture(scope="session")
def web_client(web_app):
    with web_app.test_client() as c:
        yield c
        logger.info("web client is closed")


@pytest.fixture
def web_app_ctrl(web_app, web_client):
    return WebAppCtrl(app=web_app, appbuilder=web_app.web_appbuilder, client=web_client)
