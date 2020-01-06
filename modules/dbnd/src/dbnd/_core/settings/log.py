import logging
import sys

from typing import Callable, List, Optional

from dbnd._core.configuration.environ_config import in_quiet_mode
from dbnd._core.log.config import configure_logging_dictConfig
from dbnd._core.log.logging_utils import get_sentry_logging_config, setup_log_file
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
        default=16 * 1024 * 1024,  # 16MB
        description="Max log file size in bytes to be sent to server.\n"
        "\t* use 0 for unlimited;"
        "\t* use -1 to disable;"
        "\t* use negative (e.g. -1000) to get log's 'head' instead of 'tail'."
        "Default: 16MB.",
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

    def get_dbnd_logging_config(self, filename=None):
        if self.custom_dict_config:
            dict_config = self.settings.log.custom_dict_config()
            if not in_quiet_mode():
                logger.info("Using user provided logging config")
        else:
            dict_config = self.get_dbnd_logging_config_base(filename=filename)
        return dict_config

    def get_dbnd_logging_config_base(self, filename=None):
        # type: (LoggingConfig, Optional[str]) -> Optional[dict]
        log_settings = self
        if log_settings.disabled:
            return

        log_level = log_settings.level
        console_stream = sys.stdout if log_settings.stream_stdout else sys.stderr

        # dummy path, we will not write to this file
        task_file_handler_file = databand_system_path("logs", "task.log")
        setup_log_file(task_file_handler_file)

        config = {
            "version": 1,
            "disable_existing_loggers": False,
            "filters": {
                "task_context_filter": {
                    "()": "dbnd._core.log.logging_utils.TaskContextFilter"
                }
            },
            "formatters": {
                "formatter": {"format": log_settings.formatter},
                "formatter_simple": {"format": log_settings.formatter_simple},
                "formatter_colorlog": {
                    "()": "colorlog.ColoredFormatter",
                    "format": log_settings.formatter_colorlog,
                    "reset": True,
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "stream": console_stream,
                    "formatter": log_settings.console_formatter,
                    "filters": ["task_context_filter"],
                },
                "task_file": {
                    "class": "logging.FileHandler",
                    "formatter": log_settings.file_formatter,
                    "filename": task_file_handler_file,
                    "encoding": "utf-8",
                },
            },
            "loggers": {
                "databand.task_logger": {
                    "handlers": ["task_file"],
                    "level": log_level,
                    "propagate": False,
                }
                # "airflow.task": {
                #     # same as root, we keep it as, there is validation that airflow.task log exists
                #     "handlers": ["console", "file", "exception"],
                #     "level": log_level,
                #     "propagate": False,
                # }
            },
            "root": {"handlers": ["console"], "level": log_level},
        }
        if filename:
            setup_log_file(filename)
            config["handlers"]["file"] = {
                "class": "logging.FileHandler",
                "formatter": log_settings.file_formatter,
                "filename": filename,
                "encoding": "utf-8",
            }
            config["root"]["handlers"].append("file")

        loggers = config.setdefault("loggers", {})
        for logger_warn in log_settings.at_warn:
            loggers[logger_warn] = {"level": logging.WARNING, "propagate": True}

        for logger_debug in log_settings.at_debug:
            loggers[logger_debug] = {"level": logging.DEBUG, "propagate": True}

        if log_settings.sqlalchemy_print:
            loggers["sqlalchemy.engine"] = {"level": logging.INFO, "propagate": True}

        if log_settings.sentry_url:
            config["handlers"]["sentry"] = get_sentry_logging_config(
                sentry_url=log_settings.sentry_url, sentry_env=log_settings.sentry_env
            )
            config["root"]["handlers"].append("sentry")

        return config

    def configure_dbnd_logging(self):
        if self.disabled:
            return

        dict_config = self.get_dbnd_logging_config(filename=None)
        configure_logging_dictConfig(dict_config=dict_config)
        if not in_quiet_mode():
            logger.info("Databand logging is up!")
