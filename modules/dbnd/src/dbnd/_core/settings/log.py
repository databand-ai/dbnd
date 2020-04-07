import logging
import sys

from logging.config import DictConfigurator
from typing import Callable, List, Optional

from dbnd._core.configuration.environ_config import in_quiet_mode
from dbnd._core.log.config import configure_logging_dictConfig
from dbnd._core.log.logging_utils import (
    find_handler,
    get_sentry_logging_config,
    setup_log_file,
)
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task import config
from dbnd._core.utils.basics.format_exception import format_exception_as_str
from dbnd._core.utils.project.project_fs import databand_system_path
from dbnd._vendor.tbvaccine import TBVaccine


logger = logging.getLogger(__name__)


class LoggingConfig(config.Config):
    """Databand's logger configuration"""

    _conf__task_family = "log"
    disabled = parameter(description="Should logging be disabled").value(False)
    capture_stdout_stderr = parameter(
        description="Should logger retransmit all output wrtten to stdout\stderr"
    ).value(True)
    capture_task_run_log = parameter.help("Capture task output into log").value(True)

    override_airflow_logging_on_task_run = parameter(
        description="Replace airflow logger with databand logger"
    ).value(True)
    support_jupiter = parameter(
        description="Support logging output to Jupiter UI"
    ).value(True)

    level = parameter(description="Logging level. DEBUG\INFO\WARN\ERROR").value("INFO")
    formatter = parameter(
        description="Log formatting string (logging library convention)"
    )[str]
    formatter_colorlog = parameter(
        description="Log formatting string (logging library convention)"
    )[str]
    formatter_simple = parameter(
        description="Log formatting string (logging library convention)"
    )[str]

    console_formatter_name = parameter(
        description="The name of the formatter logging to console output"
    )[str]
    file_formatter_name = parameter(
        description="The name of the formatter logging to file output"
    )[str]

    sentry_url = parameter(
        default=None, description="URL for setting up sentry logger"
    )[str]
    sentry_env = parameter(default=None, description="Envrionment for sentry logger")[
        str
    ]

    file_log = parameter(default=None, description="Log to file (off by default)")[str]

    stream_stdout = parameter(
        description="Should databand'a logger stream stdout instead of stderr"
    ).value(False)

    custom_dict_config = parameter(
        default=None, description="Advanced: Customized logging configuration"
    )[Callable]

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
    api_profile = parameter(description="profile api calls").value(False)

    def _initialize(self):
        super(LoggingConfig, self)._initialize()
        self.task_log_file_formatter = None

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
            if not in_quiet_mode():
                logger.info("Using user provided logging config")
            return self.settings.log.custom_dict_config()

        return self.get_dbnd_logging_config_base(filename=filename)

    def get_dbnd_logging_config_base(self, filename=None):
        # type: (LoggingConfig, Optional[str]) -> Optional[dict]
        log_settings = self
        log_level = log_settings.level
        # we want to have "real" output, so nothing can catch our handler
        # in opposite to what airflow is doing
        console_stream = (
            sys.__stdout__ if log_settings.stream_stdout else sys.__stderr__
        )

        if "ipykernel" in sys.modules and self.support_jupiter:
            # we can not use __stdout__ or __stderr__ as it will not be printed into jupyter web UI
            # at the same time  using sys.stdout when airflow is active is very dangerous
            # as it can create dangerous loop from airflow redirection into root logger
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
                    "()": "dbnd._vendor.colorlog.ColoredFormatter",
                    "format": log_settings.formatter_colorlog,
                    "reset": True,
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "stream": console_stream,
                    "formatter": log_settings.console_formatter_name,
                    "filters": ["task_context_filter"],
                }
            },
            "root": {"handlers": ["console"], "level": log_level},
        }
        if filename:
            setup_log_file(filename)
            config["handlers"]["file"] = {
                "class": "logging.FileHandler",
                "formatter": log_settings.file_formatter_name,
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

        dict_config = self.get_dbnd_logging_config(filename=self.file_log)

        airflow_task_log_handler = None
        if self.override_airflow_logging_on_task_run:
            airflow_task_log_handler = self.dbnd_override_airflow_logging_on_task_run()

        configure_logging_dictConfig(dict_config=dict_config)

        if airflow_task_log_handler:
            logging.root.handlers.append(airflow_task_log_handler)
        logger.debug("Databand logging is up!")

    def dbnd_override_airflow_logging_on_task_run(self):
        # EXISTING STATE:
        # root logger use Console handler -> prints to current sys.stdout
        # on `airflow run` without interactive -> we have `redirect_stderr` applied that will redirect sys.stdout
        # into logger `airflow.task`, that will save everything into file.
        #  EVERY output of root logger will go through CONSOLE handler into AIRFLOW.TASK without being printed to screen

        if not sys.stderr or not _safe_is_typeof(sys.stderr, "StreamLogWriter"):
            logger.debug(
                "Airflow logging is already replaced by dbnd stream log writer!"
            )
            return

        # NEW STATE
        # we will move airflow.task file handler to root level
        # we will set propogate
        # we will stop redirect of airflow logging

        # this will disable stdout ,stderr redirection
        sys.stderr = sys.__stderr__
        sys.stdout = sys.__stdout__

        airflow_root_console_handler = find_handler(logging.root, "console")

        if _safe_is_typeof(airflow_root_console_handler, "RedirectStdHandler"):
            # we are removing this console logger
            # this is the logger that capable to create self loop
            # as it writes to "latest" sys.stdout,
            # if you have stdout redirection into any of loggers, that will propogate into root
            # you get very busy message loop that is really hard to debug
            logging.root.handlers.remove(airflow_root_console_handler)

        airflow_task_logger = logging.getLogger("airflow.task")
        airflow_task_log_handler = find_handler(airflow_task_logger, "task")
        if airflow_task_log_handler:
            logging.root.handlers.append(airflow_task_log_handler)
            airflow_task_logger.propagate = True
            airflow_task_logger.handlers = []

        return airflow_task_log_handler

    def get_task_log_file_handler(self, log_file):
        if not self.task_log_file_formatter:
            config = self.get_dbnd_logging_config()
            configurator = DictConfigurator(config)
            file_formatter_config = configurator.config.get("formatters").get(
                self.file_formatter_name
            )
            self.task_log_file_formatter = configurator.configure_formatter(
                file_formatter_config
            )

        # "formatter": log_settings.file_formatter,
        log_file = str(log_file)
        setup_log_file(log_file)
        handler = logging.FileHandler(filename=log_file, encoding="utf-8")
        handler.setFormatter(self.task_log_file_formatter)
        handler.setLevel(self.level)
        return handler


def _safe_is_typeof(value, name):
    if not value:
        return
    return isinstance(value, object) and value.__class__.__name__.endswith(name)
