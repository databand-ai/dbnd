import logging
import os

from dbnd._core.configuration.environ_config import in_quiet_mode
from dbnd._core.errors.friendly_error.versioned_dagbag import failed_to_load_versioned_dagbag_plugin
from dbnd._core.utils.object_utils import patch_module_attr

logger = logging.getLogger(__name__)


# WE SHOULD NOT HAVE ANY dbnd imports here -- circle import:  dbnd_airflow -> airflow -> load .. -> import dbnd_airflow

def _use_databand_airflow_dagbag():
    """
    Overriding Airflow Dagbag, so versioned dags can be used
    :return:
    """
    import airflow
    from airflow import settings
    from dbnd_airflow.compat.www import views

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

    logging.info("Using DBND DagBag with support for versioned dags and historic dag runs.")


def use_databand_airflow_dagbag():
    try:
        _use_databand_airflow_dagbag()
    except Exception as e:
        raise failed_to_load_versioned_dagbag_plugin(e)


def patch_airflow_create_app():
    if not in_quiet_mode():
        logger.debug("Adding support for versioned DBND DagBag")

    def patch_create_app(create_app_func):
        def patched_create_app(*args, **kwargs):
            res = create_app_func(*args, **kwargs)
            use_databand_airflow_dagbag()
            return res

        return patched_create_app

    from dbnd_airflow.compat.www import get_apps_to_patch
    for app in get_apps_to_patch():
        app.create_app = patch_create_app(app.create_app)
