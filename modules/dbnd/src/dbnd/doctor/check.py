# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging

from dbnd import dbnd_tracking
from dbnd._core.task_build.dbnd_decorator import task
from dbnd.doctor import system_airflow, system_dbnd, system_logging, system_python
from dbnd.doctor.doctor_report_builder import DoctorStatusReportBuilder


logger = logging.getLogger(__name__)


@task
def dbnd_doctor(
    python_sanity: bool = True,
    airflow_sanity: bool = True,
    logs: bool = False,
    python_packages: bool = False,
    check_time: datetime.datetime = datetime.datetime.now(),
    env: bool = False,
    all: bool = False,
):
    if all:
        logs = python_packages = env = True

    main_report = DoctorStatusReportBuilder("Dbnd Doctor")
    main_report.log("check_time", check_time)

    system_report = system_dbnd.dbnd_status()
    logger.info("system_report: %s", system_report)

    if env:
        system_dbnd_env_report = system_dbnd.dbnd_environ()
        main_report.add_sub_report(system_dbnd_env_report)

    if python_sanity:
        system_python_report = system_python.python_status(
            python_packages=python_packages
        )
        main_report.add_sub_report(system_python_report)
    if airflow_sanity:
        airflow_report = system_airflow.airflow_status()
        main_report.add_sub_report(airflow_report)

        if env:
            airflow_environ_report = system_airflow.airflow_environ()
            main_report.add_sub_report(airflow_environ_report)

    if logs:
        system_logging_report = system_logging.logging_status()
        main_report.add_sub_report(system_logging_report)

    logger.info("Your system is good to go! Enjoy Databand!")
    return main_report.get_status_str()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with dbnd_tracking():
        dbnd_doctor(all=True)
