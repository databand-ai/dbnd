import logging

from typing import Callable, List

from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import config
from dbnd._core.utils.basics.format_exception import format_exception_as_str
from dbnd._core.utils.project.project_fs import databand_system_path
from dbnd._core.utils.string_render import StringRenderer
from dbnd._vendor.tbvaccine import TBVaccine


logger = logging.getLogger(__name__)


class LoggingConfig(config.Config):
    """Databand's logger"""

    _conf__task_family = "log"
    disabled = parameter.value(False)

    level = parameter.value("INFO")
    formatter = parameter[str]
    formatter_colorlog = parameter[str]
    formatter_simple = parameter[str]

    task_file_log = parameter.help("log file per task").value(True)

    console_formatter = parameter[str]
    file_formatter = parameter[str]
    subprocess_formatter = parameter[str]

    sentry_url = parameter(default=None)[str]
    sentry_env = parameter(default=None)[str]

    stream_stdout = parameter.value(False)

    custom_dict_config = parameter(default=None)[Callable]

    at_warn = parameter.help("name of loggers to put in WARNING mode").c[List[str]]
    at_debug = parameter.help("name of loggers to put in DEBUG mode").c[List[str]]

    exception_no_color = parameter(
        default=False, description="Do not use colors in exception handling"
    )[bool]
    exception_simple = parameter(
        default=False, description="Simple mode of exception handling"
    )[bool]

    send_body_to_server_max_size = parameter(
        default=1000000,  #
        description="Max log file size in bytes to be sent to server."
        " Use 0 for unlimited; use -1 to disable. Default: 16MB.",
    )[int]

    remote_logging_disabled = parameter.help(
        "for tasks using a cloud environment, don't copy the task log to cloud storage"
    ).value(False)

    sqlalchemy_print = parameter(description="enable sqlalchemy logger").value(False)
    sqlalchemy_trace = parameter(description="trace sqlalchemy queries").value(False)
    sqlalchemy_profile = parameter(description="profile sqlalchemy queries").value(
        False
    )

    @property
    def task_file_template_render(self):
        return StringRenderer.from_str(self.task_file_template)

    def get_pipeline_logfile(self, pipeline_name, execution_date):
        return databand_system_path(
            "logs",
            "driver",
            execution_date.strftime("%Y-%m-%d"),
            "%s__%s.log"
            % (execution_date.strftime("%Y-%m-%d__%H-%M-%S"), pipeline_name),
        )

    def format_exception_as_str(self, exc_info, isolate=True):
        if self.exception_simple:
            return format_exception_as_str(exc_info)

        try:
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
