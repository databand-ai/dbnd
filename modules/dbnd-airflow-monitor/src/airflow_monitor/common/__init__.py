from functools import wraps


def capture_monitor_exception(logger, message=None, default=None):
    def wrapper(f):
        @wraps(f)
        def wrapped(obj, *args, **kwargs):
            try:
                logger.debug("[%s] %s", obj, message or f.__name__)
                return f(obj, *args, **kwargs)
            except Exception:
                logger.exception("[%s] Error during %s", obj, message)
                return default() if callable(default) else default

        return wrapped

    return wrapper
