import logging
import os
import sys

from contextlib import contextmanager
from logging.config import dictConfig
from typing import Optional

from dbnd._core.configuration.dbnd_config import config as dbnd_config
from dbnd._core.log.logging_utils import setup_log_file


END_OF_LOG_MARK = "end_of_log"
logger = logging.getLogger(__name__)


FORMAT_FULL = "[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s"
FORMAT_SIMPLE = "%(asctime)s %(levelname)s - %(message)s"
FORMAT_COLORLOG = "[%(asctime)s] %(log_color)s%(levelname)s %(reset)s - %(message)s"


def basic_logging_config(
    filename=None,
    log_level=logging.INFO,
    console_stream=sys.stderr,
    console_formatter_name="formatter_colorlog",
    file_formatter_name="formatter_full",
):
    # type: (...) -> Optional[dict]

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "formatter_full": {"format": FORMAT_FULL},
            "formatter_simple": {"format": FORMAT_SIMPLE},
            "formatter_colorlog": {
                "()": "dbnd._vendor.colorlog.ColoredFormatter",
                "format": FORMAT_COLORLOG,
                "reset": True,
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": console_stream,
                "formatter": console_formatter_name,
            }
        },
        "root": {"handlers": ["console"], "level": log_level},
    }
    if filename:
        setup_log_file(filename)
        config["handlers"]["file"] = {
            "class": "logging.FileHandler",
            "formatter": file_formatter_name,
            "filename": filename,
            "encoding": "utf-8",
        }
        config["root"]["handlers"].append("file")

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


def configure_logging_dictConfig(dict_config):
    try:
        dictConfig(dict_config)
    except Exception:
        logging.exception("Unable to configure logging using %s!", dict_config)
        raise


def configure_basic_logging(log_file):
    """
    Simple databand logging, called from main and other commands
    """
    configure_logging_dictConfig(basic_logging_config(filename=log_file))
