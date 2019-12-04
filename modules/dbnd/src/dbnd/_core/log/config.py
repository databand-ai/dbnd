import logging
import os
import sys

from contextlib import contextmanager
from logging.config import dictConfig
from typing import Optional

from dbnd._core.log.logging_utils import capture_log_into_file, setup_log_file
from dbnd._core.settings.log import LoggingConfig
from dbnd._core.utils.project.project_fs import databand_system_path
from dbnd._core.utils.timezone import utcnow


END_OF_LOG_MARK = "end_of_log"
logger = logging.getLogger(__name__)


def get_task_logger():
    for h in logging.getLogger("databand.task_logger").handlers:
        if h.name == "task_file":
            return h
    return None


FORMAT_FULL = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
FORMAT_SIMPLE = "%(asctime)s %(levelname)s - %(message)s"
FORMAT_COLORLOG = "[%(asctime)s] %(log_color)s%(levelname)s %(reset)s - %(message)s"

ENV_SENTRY_URL = "DBND__LOG__SENTRY_URL"
ENV_SENTRY_ENV = "DBND__LOG__SENTRY_ENV"


def basic_logging_config(
    filename=None,
    log_level=logging.INFO,
    console_stream=sys.stderr,
    console_formatter="formatter_colorlog",
    file_formatter="formatter_full",
):
    # type: (...) -> Optional[dict]

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "formatter_full": {"format": FORMAT_FULL},
            "formatter_simple": {"format": FORMAT_SIMPLE},
            "formatter_colorlog": {
                "()": "colorlog.ColoredFormatter",
                "format": FORMAT_COLORLOG,
                "reset": True,
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": console_stream,
                "formatter": console_formatter,
            }
        },
        "root": {"handlers": ["console"], "level": log_level},
    }
    if filename:
        setup_log_file(filename)
        config["handlers"]["file"] = {
            "class": "logging.FileHandler",
            "formatter": file_formatter,
            "filename": filename,
            "encoding": "utf-8",
        }
        config["root"]["handlers"].append("file")

    sentry_url = os.environ.get(ENV_SENTRY_URL)
    if sentry_url:
        sentry_env = os.environ.get(ENV_SENTRY_ENV) or "dev"

        config["handlers"]["sentry"] = get_sentry_logging_config(
            sentry_url=sentry_url, sentry_env=sentry_env
        )
        config["root"]["handlers"].append("sentry")

    return config


def get_sentry_logging_config(sentry_url, sentry_env):
    import raven.breadcrumbs

    for ignore in (
        "sqlalchemy.orm.path_registry",
        "sqlalchemy.pool.NullPool",
        "raven.base.Client",
    ):
        raven.breadcrumbs.ignore_logger(ignore)

    return {
        "exception": {
            "level": "ERROR",
            "class": "raven.handlers.logging.SentryHandler",
            "dsn": sentry_url,
            "environment": sentry_env,
        }
    }


def get_dbnd_logging_config(log_settings=None, filename=None):
    # type: (LoggingConfig, ...) -> Optional[dict]

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


_current_time = utcnow()


def _get_current_time_str():
    return (
        _current_time.strftime("%Y-%m-%d"),
        _current_time.strftime("%Y-%m-%d__%H-%M-%S"),
    )


def configure_basic_logging(log_file):
    configure_logging_dictConfig(basic_logging_config(filename=log_file))


def configure_logging_dictConfig(dict_config):
    try:
        dictConfig(dict_config)
    except Exception:
        logging.exception("Unable to configure logging using %s!", dict_config)
        raise


@contextmanager
def captures_log_into_file_as_task_file(log_file):
    task_file = get_task_logger()
    if not task_file:
        yield None
        return

    with capture_log_into_file(
        log_file=log_file, formatter=task_file.formatter, level=task_file.level
    ) as handler:
        yield handler
