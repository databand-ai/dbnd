import logging
import sys

import dbnd

from dbnd import task
from dbnd.tasks.doctor.doctor_report_builder import DoctorStatusReportBuilder


logger = logging.getLogger(__name__)


@task
def python_status(python_packages=False):
    """
    Shows the status of the python system
    :param python_packages: include explicit python packages info (pip freeze)
    :return:
    """
    logger.info("Running python checks..")
    report = DoctorStatusReportBuilder("Python Status")
    report.log("dbnd.version", dbnd.__version__)
    report.log("python.version", sys.version)
    report.log("python.path", sys.path)

    try:
        import pip

        report.log("pip.version", pip.__version__)
        try:
            from pip._internal.operations import freeze
        except ImportError:  # pip < 10.0
            from pip.operations import freeze
        packages = list(freeze.freeze())
        report.log("python.total_packages", len(packages))
        report.log("dbnd.packages", [p for p in packages if "dbnd" in p])

        # we want to show all packages only if explicitly requested
        if python_packages:
            logger.info("pip freeze: %s", packages)
            packages_page_size = 20  # we want to show only X packages per metric
            for i in range(0, len(packages), packages_page_size):
                report.log(
                    "python.packages.%s" % i, packages[i : i + packages_page_size]
                )
    except Exception as ex:
        logger.exception("Failed to get packages information via pips")

        report.log("python.packages", str(ex))
        report.log("python.total_packages", 0)
    return report.get_status_str()
