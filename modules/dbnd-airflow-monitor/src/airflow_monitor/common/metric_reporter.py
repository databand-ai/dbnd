from functools import wraps

from prometheus_client import Summary


class MetricReporter:
    def __init__(self):
        self.performance = Summary(
            "dbnd_af_plugin_query_duration_seconds",
            "Airflow Export Plugin Query Run Time",
            ["airflow_instance", "method_name"],
        )
        self.sizes = Summary(
            "dbnd_af_plugin_query_result_size",
            "Airflow Export Plugin Query Result Size",
            ["airflow_instance", "method_name"],
        )
        self.dbnd_api_response_time = Summary(
            "af_monitor_dbnd_api_response_time",
            "Databand API response time",
            ["airflow_instance", "method_name"],
        )
        self.exporter_response_time = Summary(
            "af_monitor_export_response_time",
            "Airflow export plugin response time",
            ["airflow_instance", "method_name"],
        )


METRIC_REPORTER = MetricReporter()


def decorate_measure_time(inst, interface_cls: type, metric: Summary, label: str):
    label = str(label)  # just in case

    def measure_time(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            with metric.labels(label, f.__name__).time():
                return f(*args, **kwargs)

        return wrapped

    for name in dir(interface_cls):
        if not name.startswith("_"):
            setattr(inst, name, measure_time(getattr(inst, name)))
    return inst
