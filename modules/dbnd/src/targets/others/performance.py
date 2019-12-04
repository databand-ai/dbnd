import logging
import time

from functools import wraps


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
