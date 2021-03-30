import datetime
import logging

from dbnd._core.decorator.dbnd_decorator import task
from dbnd._core.tracking.metrics import log_metric


logger = logging.getLogger(__name__)


def assert_plugins():
    """
        Raises error if required tracking plugin is not installed.
        Logs plugins versions if installed.
    """
    try:
        import dbnd_airflow

        log_metric("dbnd_airflow_version", dbnd_airflow.__version__)
    except ImportError:
        logger.error("dbnd_airflow is not installed")
        raise

    try:
        import dbnd_airflow_auto_tracking

        log_metric(
            "dbnd_airflow_auto_tracking_version", dbnd_airflow_auto_tracking.__version__
        )
    except ImportError:
        logger.error("dbnd_airflow_auto_tracking is not installed")
        raise

    try:
        import dbnd_airflow_export

        log_metric("dbnd_airflow_export_version", dbnd_airflow_export.__version__)
    except ImportError:
        logger.warning("dbnd_airflow_export is not installed")


# Checks if Airflow system is configured correctly for DBND Tracking.
@task
def sanity_check_for_dbnd_airflow_tracking(check_time=datetime.datetime.now()):
    # type: ( datetime.datetime)-> str
    logger.info("Running Databand Tracking Sanity Check!")

    assert_plugins()

    log_metric("Happiness Level", "1001")
    logger.info("Your Airflow system is good to go! Enjoy Databand!")

    return "Databand checked at %s" % check_time
