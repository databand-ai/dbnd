# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging

from dbnd import parameter
from dbnd._core.task_build.dbnd_decorator import task
from dbnd.tasks.doctor import system_airflow, system_dbnd, system_logging, system_python
from dbnd.tasks.doctor.doctor_report_builder import DoctorStatusReportBuilder


logger = logging.getLogger(__name__)


@task
def dbnd_doctor(
    python_sanity=parameter.value(True)[bool],
    airflow_sanity=parameter.value(True)[bool],
    logs=parameter.value(None)[bool],
    python_packages=parameter.value(None)[bool],
    check_time=datetime.datetime.now(),
    all=False,
):
    if all:
        # change only "none" params
        logs = True if logs is None else logs
        python_packages = True if python_packages is None else python_packages

    main_report = DoctorStatusReportBuilder("Dbnd Doctor")
    main_report.log("check_time", check_time)

    system_report = system_dbnd.dbnd_status()
    logger.debug("system_report: %s", system_report)

    if python_sanity:
        system_python_report = system_python.python_status(
            python_packages=python_packages
        )
        main_report.add_sub_report(system_python_report)
    if airflow_sanity:
        airflow_report = system_airflow.airflow_status()
        main_report.add_sub_report(airflow_report)
    if logs:
        system_logging_report = system_logging.logging_status()
        main_report.add_sub_report(system_logging_report)

    logger.info("Your system is good to go! Enjoy Databand!")
    return main_report.get_status_str()
