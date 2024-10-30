# © Copyright Databand.ai, an IBM Company 2022

import logging
import sys

from dbnd_monitor.base_monitor_config import BaseMonitorConfig
from dbnd_monitor.remote_monitor_log_handler import RemoteMonitorLogHandler


try:
    from .json_formatter import JsonFormatter
except ImportError:
    JsonFormatter = None


def configure_logging(use_json: bool):
    if use_json and JsonFormatter:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s %(name)s %(process)s %(threadName)s : %(message)s"
        )
    log_handler = logging.StreamHandler(stream=sys.stdout)
    log_handler.setFormatter(formatter)
    # need to reset dbnd logger, remove after dbnd._core removed
    logging.root.handlers.clear()
    logging.root.addHandler(log_handler)
    logging.root.setLevel(logging.INFO)


def configure_sending_monitor_logs(
    monitor_type: str, syncer_name: str, monitor_config: BaseMonitorConfig
):
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s %(process)s %(threadName)s : %(message)s"
    )

    httpHandler = RemoteMonitorLogHandler(
        monitor_type,
        syncer_name,
        monitor_config.max_logs_buffer_size_in_kb,
        monitor_config.max_time_delta_to_send_monitor_logs_in_minutes,
    )
    httpHandler.setLevel(logging.INFO)
    httpHandler.setFormatter(formatter)
    logging.root.addHandler(httpHandler)
