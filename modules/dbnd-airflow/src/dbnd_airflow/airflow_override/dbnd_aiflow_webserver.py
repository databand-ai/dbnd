import logging
import os

logger = logging.getLogger(__name__)


# WE SHOULD NOT HAVE ANY dbnd imports here -- circle import:  dbnd_airflow -> airflow -> load .. -> import dbnd_airflow

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


def patch_airflow_create_app():
    logger.info("Adding support for versioned DBND DagBag")
    from airflow.www_rbac import app as airflow_app

    def patch_create_app(create_app_func):
        def patched_create_app(*args, **kwargs):
            from dbnd._core.configuration.dbnd_config import config
            from dbnd_airflow._plugin import configure_airflow_sql_alchemy_conn
            from dbnd_airflow.bootstrap import _register_sqlachemy_local_dag_job

            _register_sqlachemy_local_dag_job()

            logger.info("Setting SQL connection")
            config.load_system_configs()
            configure_airflow_sql_alchemy_conn()

            res = create_app_func(*args, **kwargs)
            use_databand_airflow_dagbag()
            return res

        return patched_create_app

    airflow_app.create_app = patch_create_app(airflow_app.create_app)
