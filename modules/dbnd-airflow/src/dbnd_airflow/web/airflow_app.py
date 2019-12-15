import logging
import os

from dbnd import new_dbnd_context


def databand_context():
    with new_dbnd_context(name="webserver", autoload_modules=False) as context:
        yield context


def use_databand_airflow_dagbag():
    """
    Overriding Airflow Dagbag, so versioned dags can be used
    :return:
    """

    import airflow
    from airflow import settings
    from airflow.www_rbac import views
    from dbnd_airflow.airflow_override import patch_module_attr

    logging.info("Using dbnd dagbag (supporting versioned dags).")
    from dbnd_airflow.web.databand_versioned_dagbag import DbndAirflowDagBag

    if os.environ.get("SKIP_DAGS_PARSING") != "True":
        views.dagbag = DbndAirflowDagBag(settings.DAGS_FOLDER)
    else:
        views.dagbag = DbndAirflowDagBag(os.devnull, include_examples=False)

    # dag_details invoke DagBag directly
    patch_module_attr(airflow.models, "DagBag", DbndAirflowDagBag)
    patch_module_attr(airflow.models.dag, "DagBag", DbndAirflowDagBag)


def create_app(config=None, testing=False):
    from airflow.www_rbac import app as airflow_app

    app, appbuilder = airflow_app.create_app(config=config, testing=testing)

    # only now we can load view..
    use_databand_airflow_dagbag()
    logging.info("Airflow applications has been created")
    return app, appbuilder


def create_gunicorn_app(config=None, testing=None):
    from dbnd import dbnd_bootstrap

    dbnd_bootstrap()

    return create_app(config=config, testing=testing)[0]


def cached_appbuilder(config=None, testing=False):
    _, appbuilder = create_app(config, testing)
    return appbuilder
