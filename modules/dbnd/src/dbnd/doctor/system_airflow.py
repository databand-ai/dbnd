# © Copyright Databand.ai, an IBM Company 2022

import logging
import os

from dbnd import task
from dbnd.doctor.doctor_report_builder import DoctorStatusReportBuilder


logger = logging.getLogger(__name__)


@task
def airflow_status():
    """
    Raises error if required tracking plugin is not installed.
    Logs plugins versions if installed.
    """
    report = DoctorStatusReportBuilder("Airflow Status")
    logger.info("Checking Databand+Airflow installation")

    report.log("env.AIRFLOW_HOME", os.environ.get("AIRFLOW_HOME"))
    try:
        import dbnd_airflow

        report.log("dbnd_airflow.version", dbnd_airflow.__version__)
    except ImportError:
        report.log("dbnd_airflows.version", 0)
        logger.error("dbnd_airflow is not installed")

    try:
        import dbnd_airflow_auto_tracking

        report.log(
            "dbnd_airflow_auto_tracking.version", dbnd_airflow_auto_tracking.__version__
        )
    except ImportError:

        report.log("dbnd_airflow_auto_tracking.version", 0)
        logger.error("dbnd_airflow_auto_tracking is not installed")

    try:
        import airflow

        report.log("airflow.version", airflow.__version__)
    except ImportError:
        report.log("airflow.version", 0)
        logger.error("airflow is not installed")

    return report.get_status_str_and_print()


@task
def airflow_environ():
    report = DoctorStatusReportBuilder("AIRFLOW ENV Status")
    for k, v in os.environ.items():
        if k.startswith("AIRFLOW_"):
            report.log(k, v)

    return report.get_status_str_and_print()
