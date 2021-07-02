import logging

from functools import wraps
from time import time


logger = logging.getLogger(__name__)


def remote_debugger(port=15123):
    import pydevd

    pydevd.settrace("localhost", port=port, stdoutToServer=True, stderrToServer=True)


def log_call(name=None, log_f=None, print_start=False):
    log_f = log_f or logger.info

    def decorator(f):
        log_name = name or f.__name__

        @wraps(f)
        def wrapper(*args, **kwargs):
            if print_start:
                log_f("FUNC %s: starting", log_name)
            start = time()
            try:
                return f(*args, **kwargs)
            finally:
                end = time()
                log_f("FUNC %s: elapsed time: %s", log_name, end - start)

        return wrapper

    return decorator
