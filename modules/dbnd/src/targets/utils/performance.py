# © Copyright Databand.ai, an IBM Company 2022

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
    from dbnd import log_metric

    log_level = get_target_logging_level()

    path = target
    if hasattr(target, "target"):
        path = target.target
    if hasattr(target, "path"):
        path = target.path

    name = target.source.name if target.source else path

    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        delta_ms = (end_time - start_time) * 1000
        logger.log(
            level=log_level,
            msg="Total {} time for target {} is {} milliseconds".format(
                operation, path, delta_ms
            ),
        )
        log_metric("marshalling_{}".format(name), delta_ms, source="system")
