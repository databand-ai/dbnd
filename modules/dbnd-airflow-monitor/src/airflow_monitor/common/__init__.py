from functools import wraps
from typing import Any


def capture_monitor_exception(logger, message):
    def wrapper(f):
        @wraps(f)
        def wrapped(obj, *args, **kwargs):
            # type: (Any, Any, Any) -> None
            try:
                logger.debug("[%s] %s", obj, message)
                return f(obj, *args, **kwargs)
            except Exception:
                logger.exception("[%s] Error during %s", obj, message)

        return wrapped

    return wrapper
