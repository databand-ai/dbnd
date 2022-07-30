# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd import log_metric
from dbnd._core.utils.basics.text_banner import TextBanner


logger = logging.getLogger(__name__)


class DoctorStatusReportBuilder(object):
    def __init__(self, name):
        self.name = name
        msg = "Running %s Status Check:" % name
        logger.info(msg)
        self.text_banner = TextBanner(msg)

    def log(self, key, value):
        log_metric(key, value)

        self.text_banner.column(key, value)

    def add_sub_report(self, sub_report_str):
        self.text_banner.write(sub_report_str)
        self.text_banner.write("\n")

    def get_status_str(self):
        return self.text_banner.get_banner_str()
