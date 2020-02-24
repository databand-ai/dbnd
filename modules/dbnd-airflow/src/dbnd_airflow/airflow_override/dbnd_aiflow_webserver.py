import logging
import os

from dbnd._core.configuration.environ_config import in_quiet_mode

logger = logging.getLogger(__name__)


# WE SHOULD NOT HAVE ANY dbnd imports here -- circle import:  dbnd_airflow -> airflow -> load .. -> import dbnd_airflow

def use_databand_airflow_dagbag():
    """
    Overriding Airflow Dagbag, so versioned dags can be used
    :return:
    """

    import airflow
    from airflow import settings
    if settings.RBAC:
        from airflow.www_rbac import views
    else:
        from airflow.www import views
    from dbnd_airflow.airflow_override import patch_module_attr

    logging.info("Using dbnd dagbag (supporting versioned dags).")
    from dbnd_airflow.web.databand_versioned_dagbag import DbndAirflowDagBag, DbndDagModel

    if os.environ.get("SKIP_DAGS_PARSING") != "True":
        views.dagbag = DbndAirflowDagBag(settings.DAGS_FOLDER)
    else:
        views.dagbag = DbndAirflowDagBag(os.devnull, include_examples=False)

    # some views takes dag from dag model
    if hasattr(views, "DagModel"):
        patch_module_attr(views, "DagModel", DbndDagModel)

    # dag_details invoke DagBag directly
    patch_module_attr(airflow.models, "DagBag", DbndAirflowDagBag)
    patch_module_attr(airflow.models.dag, "DagBag", DbndAirflowDagBag)
    patch_module_attr(airflow.models.dag, "DagModel", DbndDagModel)


def patch_airflow_create_app():
    if not in_quiet_mode():
        logger.debug("Adding support for versioned DBND DagBag")

    def patch_create_app(create_app_func):
        def patched_create_app(*args, **kwargs):
            from dbnd._core.configuration.dbnd_config import config
            from dbnd_airflow._plugin import configure_airflow_sql_alchemy_conn
            from dbnd_airflow.bootstrap import _register_sqlachemy_local_dag_job

            _register_sqlachemy_local_dag_job()

            logger.info("Setting SQL from databand configuration.")
            config.load_system_configs()
            configure_airflow_sql_alchemy_conn()

            res = create_app_func(*args, **kwargs)
            try:
                use_databand_airflow_dagbag()
            except Exception:
                logger.info("Failed to apply dbnd versioned dagbag")
            return res

        return patched_create_app

    from airflow.www_rbac import app as airflow_app_rbac
    airflow_app_rbac.create_app = patch_create_app(airflow_app_rbac.create_app)

    from airflow.www import app as airflow_app_no_rbac
    airflow_app_no_rbac.create_app = patch_create_app(airflow_app_no_rbac.create_app)
