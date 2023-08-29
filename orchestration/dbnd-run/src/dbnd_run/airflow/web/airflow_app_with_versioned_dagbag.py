# © Copyright Databand.ai, an IBM Company 2022
from __future__ import absolute_import

import logging
import os

from dbnd._core.log import dbnd_log_debug
from dbnd._core.utils.object_utils import patch_module_attr
from dbnd_run.airflow.compat import AIRFLOW_VERSION_2
from dbnd_run.errors.versioned_dagbag import failed_to_load_versioned_dagbag_plugin


logger = logging.getLogger(__name__)


# WE SHOULD NOT HAVE ANY dbnd imports here -- circle import:  dbnd_airflow -> airflow -> load .. -> import dbnd_airflow


def _use_databand_airflow_dagbag(app, views):
    """
    Overriding Airflow Dagbag, so versioned dags can be used
    :return:
    """
    import airflow

    from airflow import settings

    from dbnd_run.airflow.web.databand_versioned_dagbag import (
        DbndAirflowDagBag,
        DbndDagModel,
    )

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

    logging.info(
        "Using DBND DagBag with support for versioned dags and historic dag runs."
    )

    if hasattr(app, "dag_bag"):
        if os.environ.get("SKIP_DAGS_PARSING") == "True":
            app.dag_bag = DbndAirflowDagBag(os.devnull, include_examples=False)
        else:
            from airflow.settings import DAGS_FOLDER

            app.dag_bag = DbndAirflowDagBag(DAGS_FOLDER, read_dags_from_db=True)


def patch_airflow_create_app():
    dbnd_log_debug("Adding support for versioned DBND DagBag")

    def patch_create_app_in_module(web_module):
        original_create_app_func = web_module.create_app
        if hasattr(web_module, "_patched_versioned_dag"):
            return web_module

        def patched_create_app(*args, **kwargs):
            res = original_create_app_func(*args, **kwargs)
            if AIRFLOW_VERSION_2:
                from airflow.www import views

                app = res
            else:
                from airflow import settings

                if settings.RBAC:
                    from airflow.www_rbac import views
                else:
                    from airflow.www import views
                app = res[0]

            try:
                _use_databand_airflow_dagbag(app, views)
            except Exception as e:
                raise failed_to_load_versioned_dagbag_plugin(e)

            return res

        web_module.create_app = patched_create_app
        setattr(web_module, "_patched_versioned_dag", True)
        return web_module

    if AIRFLOW_VERSION_2:
        from airflow.www import app as airflow_app

        patch_create_app_in_module(airflow_app)
    else:
        from airflow.www_rbac import app as airflow_app_rbac

        patch_create_app_in_module(airflow_app_rbac)

        from airflow.www import app as airflow_app_no_rbac

        patch_create_app_in_module(airflow_app_no_rbac)


def create_app(config=None, testing=False):
    patch_airflow_create_app()

    if AIRFLOW_VERSION_2:
        from airflow.www import app as airflow_app

        app = airflow_app.create_app(config=config, testing=testing)
        appbuilder = app.appbuilder
    else:
        from airflow.www_rbac import app as airflow_app

        app, appbuilder = airflow_app.create_app(config=config, testing=testing)

    logging.info("Airflow applications has been created")
    return app, appbuilder
