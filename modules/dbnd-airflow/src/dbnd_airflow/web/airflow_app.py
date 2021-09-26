import logging


def create_app(config=None, testing=False):
    from airflow.www_rbac import app as airflow_app

    app, appbuilder = airflow_app.create_app(config=config, testing=testing)

    # only now we can load view..
    # this import might causes circular dependency if placed above
    from dbnd_airflow.airflow_override.dbnd_aiflow_webserver import (
        use_databand_airflow_dagbag,
    )

    use_databand_airflow_dagbag()
    logging.info("Airflow applications has been created")
    return app, appbuilder


def cached_appbuilder(config=None, testing=False):
    _, appbuilder = create_app(config, testing)
    return appbuilder
