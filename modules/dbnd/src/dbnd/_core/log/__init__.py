import logging

from dbnd._core.current import is_verbose


logger = logging.getLogger(__name__)


def dbnd_log_debug(msg, *args, **kwargs):
    try:
        if is_verbose():
            logger.info(msg, *args, **kwargs)
        else:
            logger.debug(msg, *args, **kwargs)
    except:
        print("Failed to print dbnd info message")


def dbnd_log_info_error(msg, *args, **kwargs):
    """we show exception only in verbose mode"""
    try:
        if is_verbose():
            logger.exception(msg, *args, **kwargs)
        else:
            logger.info(msg, *args, **kwargs)
    except Exception:
        print("Failed to print dbnd error message")
