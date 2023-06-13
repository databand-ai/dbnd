# © Copyright Databand.ai, an IBM Company 2022

import logging
import sys


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
