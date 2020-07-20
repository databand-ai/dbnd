import logging
import time

from functools import wraps

from dbnd._core.current import get_target_logging_level
from dbnd._core.utils.seven import contextlib


logger = logging.getLogger(__name__)


def target_timeit(method):
    @wraps(method)
    def wrapper(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        state_obj = args[0]
        if hasattr(state_obj, "target"):
            state_obj = state_obj.target
        if hasattr(state_obj, "path"):
            state_obj = state_obj.path

        logger.debug("%r %s %2.2f ms" % (method.__name__, state_obj, (te - ts) * 1000))
        return result

    return wrapper


@contextlib.contextmanager
def target_timeit_log(target, operation):
    log_level = get_target_logging_level()

    name = target
    if hasattr(target, "target"):
        name = target.target
    if hasattr(target, "path"):
        name = target.path

    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        delta = end_time - start_time
        logger.log(
            level=log_level,
            msg="Total {} time for target {} is {} milliseconds".format(
                operation, name, delta * 1000
            ),
        )
