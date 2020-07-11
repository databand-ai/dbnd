import logging

from dbnd._core.current import is_verbose


logger = logging.getLogger(__name__)


def dbnd_log_debug(msg, *args, **kwargs):
    if is_verbose():
        logger.info(msg, *args, **kwargs)
    else:
        logger.debug(msg, *args, **kwargs)
