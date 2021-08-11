import logging

from airflow_monitor.common.metric_reporter import METRIC_REPORTER


logger = logging.getLogger(__name__)


def log_received_tasks(url, fetched_data):
    if not fetched_data:
        return
    try:
        d = sorted(
            [
                (k, len(v))
                for k, v in fetched_data.items()
                if hasattr(v, "__len__") and not isinstance(v, str)
            ]
        )
        if "since" in fetched_data:
            d.append(("since", fetched_data["since"]))
        logger.info(
            "Received data from %s with: {%s}",
            url,
            ", ".join(["{}: {}".format(k, v) for k, v in d]),
        )
    except Exception as e:
        logging.warning("Could not log received data. %s", e)


def send_metrics(airflow_instance_label, fetched_data):
    if not fetched_data:
        return

    try:
        metrics = fetched_data.get("metrics")
        logger.debug("Received metrics from airflow plugin: %s", metrics)

        observe_many(
            airflow_instance_label,
            METRIC_REPORTER.performance,
            metrics.get("performance", None),
        )
        observe_many(
            airflow_instance_label, METRIC_REPORTER.sizes, metrics.get("sizes", None)
        )

    except Exception as e:
        logger.error("Failed to send plugin metrics. %s", e)


def observe_many(airflow_instance_label, summary, data):
    if not data:
        return

    for metric_name, value in data.items():
        summary.labels(airflow_instance_label, metric_name.lstrip("_"),).observe(value)
