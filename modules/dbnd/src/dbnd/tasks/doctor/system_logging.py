from __future__ import print_function

import logging
import sys

from dbnd._core.current import try_get_databand_context
from dbnd._core.task_build.dbnd_decorator import task
from dbnd.tasks.doctor.doctor_report_builder import DoctorStatusReportBuilder


logger = logging.getLogger(__name__)


@task
def logging_status():
    # type: ()->str
    """
      Shows the status of the logging system
      All known loggers, logging configuration and so on.
      :return:
    """
    report = DoctorStatusReportBuilder("Logging Status")

    report.log("logging.root", logging.root)
    report.log("logging.root.handlers", logging.root.handlers)
    report.log("logger", logger)
    report.log("logger.handlers", logger.handlers)

    # airflow usually alternate stderr/stdout
    report.log("sys.stderr", sys.stderr)
    report.log("sys.stderr[close]", hasattr(sys.stderr, "close"))
    report.log("sys.stderr", sys.__stderr__)
    report.log("sys.__stderr__[close]", hasattr(sys.__stderr__, "close"))

    dbnd_context = try_get_databand_context()
    if dbnd_context:
        from dbnd._core.task_ctrl.task_visualiser import TaskVisualiser

        report.add_sub_report(
            TaskVisualiser(dbnd_context.settings.log).banner("Log Config")
        )
    # check airflow logging

    try:
        from logging import Logger

        airflow_task_logger = Logger.manager.loggerDict.get("airflow.task")
        if airflow_task_logger:
            report.log("Airlfow task logger", airflow_task_logger)
            report.log("Airlfow task logger handlers", airflow_task_logger.handlers)
        else:
            report.log("Airlfow task logger", "not found")
    except Exception as ex:
        ex_msg = "Failed to get airlfow.task logger status: %s" % ex
        report.log("Airflow task logger", ex_msg)
        logger.exception(ex_msg)

    logging_status = report.get_status_str()
    logging_status = "\n{sep}\n{msg}\n{sep}s\n".format(msg=logging_status, sep="*" * 40)
    logger.info(logging_status)
    # if we run this check we might have a problem with logs, we don't know how we are going to see the message
    print(
        "\n\nLogging Status (via __stderr__)%s" % logging_status, file=sys.__stderr__,
    )

    logger.info("Running logging validation.. (you will see a lot of messages)")

    # now we can print things, it might be that one of them will "kill the process"
    # because of some weird log handlers loop
    print("Message via print")
    print("Message via print stderr", file=sys.stderr)
    print("Message via print __stderr__", file=sys.__stderr__)
    logging.info("Message via logging root")
    logger.info("Message via logger")

    return logging_status
