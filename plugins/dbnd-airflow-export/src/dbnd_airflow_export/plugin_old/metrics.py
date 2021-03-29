from functools import wraps
from timeit import default_timer

import flask


def measure_time(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        start = default_timer()
        result = f(*args, **kwargs)
        end = default_timer()
        if flask._app_ctx_stack.top is not None:
            if "perf_metrics" not in flask.g:
                flask.g.perf_metrics = {}
            flask.g.perf_metrics[f.__name__] = end - start
        return result

    return wrapped


def save_result_size(*names):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            result = f(*args, **kwargs)
            if flask._app_ctx_stack.top is not None:
                values = result
                if len(names) == 1:
                    values = (result,)
                for name, value_list in zip(names, values):
                    if "size_metrics" not in flask.g:
                        flask.g.size_metrics = {}
                    flask.g.size_metrics[name] = len(value_list)
            return result

        return wrapped

    return decorator
