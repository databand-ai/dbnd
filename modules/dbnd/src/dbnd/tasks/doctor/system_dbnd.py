import logging
import os
import random

from dbnd import log_metric, task
from dbnd._core.current import try_get_databand_context
from dbnd.tasks.doctor.doctor_report_builder import DoctorStatusReportBuilder


logger = logging.getLogger(__name__)


@task
def dbnd_status():
    report = DoctorStatusReportBuilder("Databand Status")

    report.log("env.DBND_HOME", os.environ.get("DBND_HOME"))
    dc = try_get_databand_context()
    report.log("DatabandContext", dc)
    if dc:
        report.log("initialized", dc)

    # calling metrics.
    log_metric("metric_check", "OK")
    log_metric("metric_random_value", random.random())
    return report.get_status_str()
