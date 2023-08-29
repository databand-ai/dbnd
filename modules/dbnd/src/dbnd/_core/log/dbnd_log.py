# © Copyright Databand.ai, an IBM Company 2022

import logging
import sys

from dbnd._core.utils.basics.environ_utils import environ_enabled


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


logger = logging.getLogger(__name__)

ENV_DBND__VERBOSE = "DBND__VERBOSE"  # VERBOSE
_VERBOSE = None


DBND_MSG_MARKER = "DBND: "


def is_verbose():
    return _VERBOSE


def set_verbose(verbose: bool = True):
    """Enable Verbose output from DBND SDK."""
    global _VERBOSE
    _VERBOSE = verbose
    if _VERBOSE:
        eprint(DBND_MSG_MARKER + "VERBOSE mode is ON.")
        dbnd_logger = logging.getLogger("dbnd")
        if is_verbose() and dbnd_logger.level == logging.WARNING:
            dbnd_logger.setLevel(logging.INFO)


# we do it during the import, as there are some code (patches/wrappers) that might use this variable
set_verbose(environ_enabled(ENV_DBND__VERBOSE))


def dbnd_log_debug(msg, *args, **kwargs):
    # not every logging is configured to show .debug messages.
    # we can't change user logging, so we switch to .info by ourself.

    try:
        if is_verbose():
            logger.info(DBND_MSG_MARKER + msg, *args, **kwargs)
        else:
            logger.debug(DBND_MSG_MARKER + msg, *args, **kwargs)
    except:
        eprint(DBND_MSG_MARKER + "Failed to print dbnd info message")


def dbnd_log_info(msg, *args, **kwargs):
    try:
        logger.info(DBND_MSG_MARKER + msg, *args, **kwargs)
    except:
        eprint(DBND_MSG_MARKER + "Failed to print dbnd info message")


def dbnd_log_exception(msg, *args, **kwargs):
    """we show exception only in verbose mode"""
    try:
        if is_verbose():
            logger.exception(DBND_MSG_MARKER + msg, *args, **kwargs)
        else:
            logger.info(DBND_MSG_MARKER + msg, *args, **kwargs)
    except Exception:
        eprint(DBND_MSG_MARKER + "Failed to print dbnd error message")


def dbnd_log_init_msg(msg):
    if is_verbose():
        eprint("DBND __init__: %s" % msg)
