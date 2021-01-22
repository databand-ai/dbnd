import logging

from dbnd_airflow.airflow_override.dbnd_aiflow_webserver import (
    use_databand_airflow_dagbag,
)


def create_app(config=None, testing=False):
    from dbnd_airflow.compat.www import get_app_for_create_app

    airflow_app = get_app_for_create_app()

    app, appbuilder = airflow_app.create_app(config=config, testing=testing)

    # only now we can load view..
    use_databand_airflow_dagbag()
    logging.info("Airflow applications has been created")
    return app, appbuilder


def cached_appbuilder(config=None, testing=False):
    _, appbuilder = create_app(config, testing)
    return appbuilder
