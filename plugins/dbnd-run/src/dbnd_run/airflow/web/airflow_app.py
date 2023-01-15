# © Copyright Databand.ai, an IBM Company 2022
from __future__ import absolute_import

import logging

from dbnd_run.airflow.compat import AIRFLOW_VERSION_2


def create_app(config=None, testing=False):
    if AIRFLOW_VERSION_2:
        from airflow.www import app as airflow_app

        app = airflow_app.create_app(config=config, testing=testing)
        appbuilder = app.appbuilder
    else:
        from airflow.www_rbac import app as airflow_app

        app, appbuilder = airflow_app.create_app(config=config, testing=testing)

    # only now we can load view..
    # this import might causes circular dependency if placed above
    from dbnd_run.airflow.plugins.dbnd_aiflow_webserver import (
        use_databand_airflow_dagbag,
    )

    use_databand_airflow_dagbag()
    logging.info("Airflow applications has been created")
    return app, appbuilder
