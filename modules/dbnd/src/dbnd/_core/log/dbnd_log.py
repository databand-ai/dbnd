import logging
import sys

from dbnd._core.utils.basics.environ_utils import environ_enabled


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


logger = logging.getLogger(__name__)

ENV_DBND__VERBOSE = "DBND__VERBOSE"  # VERBOSE
_VERBOSE = environ_enabled(ENV_DBND__VERBOSE)


def is_verbose():
    return _VERBOSE


def set_verbose(verbose: bool = True):
    global _VERBOSE
    _VERBOSE = verbose


def dbnd_log_debug(msg, *args, **kwargs):
    try:
        if is_verbose():
            logger.info(msg, *args, **kwargs)
        else:
            logger.debug(msg, *args, **kwargs)
    except:
        eprint("Failed to print dbnd info message")


def dbnd_log_info_error(msg, *args, **kwargs):
    """we show exception only in verbose mode"""
    try:
        if is_verbose():
            logger.exception(msg, *args, **kwargs)
        else:
            logger.info(msg, *args, **kwargs)
    except Exception:
        eprint("Failed to print dbnd error message")


def dbnd_log_exception(msg, *args, **kwargs):
    """we show exception only in verbose mode"""
    try:
        if is_verbose():
            logger.exception(msg, *args, **kwargs)
        else:
            logger.info(msg, *args, **kwargs)
    except Exception:
        eprint("Failed to print dbnd error message")


def dbnd_log_tracking(msg, *args, **kwargs):
    if _VERBOSE:
        eprint("DBND TRACKING: %s" % msg)


def dbnd_log_init_msg(msg):
    if _VERBOSE:
        eprint("DBND INIT: %s" % msg)
