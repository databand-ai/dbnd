import logging
import signal


logger = logging.getLogger(__name__)


def safe_signal(signalnum, handler):
    # wraps signal cmd, so it doesn't throws an exception
    # while running on windows/multithreaded environment
    try:
        return signal.signal(signalnum, handler)
    except Exception:
        logger.info("Failed to set an alert handler for '%s' signal", signalnum)
    return None
