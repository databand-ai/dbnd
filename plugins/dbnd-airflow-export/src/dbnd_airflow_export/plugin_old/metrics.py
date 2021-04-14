from collections import defaultdict
from contextlib import contextmanager
from functools import wraps
from timeit import default_timer

import flask


class MetricCollector(object):
    def __init__(self):
        self.d = None

    @contextmanager
    def use_local(self):
        if self.d is not None:
            # already local mode
            yield self.d
            return

        self.d = defaultdict(dict)
        try:
            yield self.d
        finally:
            self.d = None

    def add(self, group, name, value):
        if self.d is not None:
            self.d[group][name] = value
        elif flask.has_app_context():
            metrics = getattr(flask.g, group, None)
            if metrics is None:
                metrics = {}
                setattr(flask.g, group, metrics)
            metrics[name] = value


METRIC_COLLECTOR = MetricCollector()


def measure_time(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        start = default_timer()
        result = f(*args, **kwargs)
        end = default_timer()
        METRIC_COLLECTOR.add("perf_metrics", f.__name__, end - start)
        return result

    return wrapped


def save_result_size(*names):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            result = f(*args, **kwargs)
            values = result
            if len(names) == 1:
                values = (result,)
            for name, value_list in zip(names, values):
                METRIC_COLLECTOR.add("size_metrics", name, len(value_list))
            return result

        return wrapped

    return decorator
