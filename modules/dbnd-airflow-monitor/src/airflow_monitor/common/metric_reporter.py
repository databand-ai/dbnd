from functools import wraps

from prometheus_client import CollectorRegistry, Summary, generate_latest


registry = CollectorRegistry()


class MetricReporter:
    def __init__(self):
        self._performance = None
        self._sizes = None
        self._dbnd_api_response_time = None
        self._exporter_response_time = None

    @property
    def performance(self):
        if self._performance is None:
            self._performance = Summary(
                "dbnd_af_plugin_query_duration_seconds",
                "Airflow Export Plugin Query Run Time",
                ["airflow_instance", "method_name"],
                registry=registry,
            )
        return self._performance

    @property
    def sizes(self):
        if self._sizes is None:
            self._sizes = Summary(
                "dbnd_af_plugin_query_result_size",
                "Airflow Export Plugin Query Result Size",
                ["airflow_instance", "method_name"],
                registry=registry,
            )
        return self._sizes

    @property
    def dbnd_api_response_time(self):
        if self._dbnd_api_response_time is None:
            self._dbnd_api_response_time = Summary(
                "af_monitor_dbnd_api_response_time",
                "Databand API response time",
                ["airflow_instance", "method_name"],
                registry=registry,
            )
        return self._dbnd_api_response_time

    @property
    def exporter_response_time(self):
        if self._exporter_response_time is None:
            self._exporter_response_time = Summary(
                "af_monitor_export_response_time",
                "Airflow export plugin response time",
                ["airflow_instance", "method_name"],
                registry=registry,
            )
        return self._exporter_response_time


METRIC_REPORTER = MetricReporter()


def generate_latest_metrics():
    return generate_latest(registry)


def measure_time(metric: Summary, label: str):
    label = str(label)  # just in case

    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            with metric.labels(label, f.__name__).time():
                return f(*args, **kwargs)

        return wrapped

    return decorator


def decorate_methods(inst, interface_cls: type, *decorators):
    for name in dir(interface_cls):
        if not name.startswith("_"):
            f = getattr(inst, name)
            if not callable(f):
                continue
            for decorator in decorators:
                f = decorator(f)
            setattr(inst, name, f)
    return inst
