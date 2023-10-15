# © Copyright Databand.ai, an IBM Company 2022

from __future__ import print_function

import logging

from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import config
from dbnd._core.utils.basics.format_exception import format_exception_as_str


logger = logging.getLogger(__name__)


class TrackingLoggingConfig(config.Config):
    """Databand's logger configuration"""

    _conf__task_family = "log"

    # user still need to enable preview_head_bytes/preview_tail_bytes
    # the moment we change it to False (and change preview_*)
    # we should update Airflow Monitor webservice
    capture_tracking_log = parameter(
        default=True, description="Enable log capturing for tracking tasks."
    )[bool]

    preview_head_bytes = parameter(
        default=0,  # Disabled
        description="Determine the maximum head size of the log file, bytes to be sent to server.\n"
        " The default value is 0 Kilobytes.",
    )[int]

    preview_tail_bytes = parameter(
        default=0,  # Disabled
        description="Determine the maximum tail size of the log file, bytes to be sent to server.\n"
        " The default value is 0 Kilobytes",
    )[int]

    exception_no_color = parameter(
        default=False, description="Disable using colors in exception handling."
    )[bool]
    exception_simple = parameter(
        default=False, description="Use simple mode of exception handling"
    )[bool]

    capture_stdout_stderr = parameter(
        description="Set if logger should retransmit all output written to stdout or stderr"
    ).value(True)

    formatter = parameter(
        description="Set the log formatting string, using the logging library convention."
    )[str]

    console_value_preview_size = parameter(
        description="Maximum length of string previewed in TaskVisualiser"
    )[int]

    def format_exception_as_str(self, exc_info, isolate=True):
        if self.exception_simple:
            return format_exception_as_str(exc_info)

        try:
            from dbnd._vendor.tbvaccine import TBVaccine

            tbvaccine = TBVaccine(
                no_colors=self.exception_no_color,
                show_vars=False,
                skip_non_user_on_isolate=True,
                isolate=isolate,
            )
            return tbvaccine.format_tb(*exc_info)
        except Exception as ex:
            logger.info("Failed to format exception: %s", ex)
            return format_exception_as_str(exc_info)
