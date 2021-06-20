import json
import logging
import sys
import traceback

from tempfile import NamedTemporaryFile
from time import sleep

from airflow_monitor.airflow_data_saving import save_airflow_server_info
from airflow_monitor.common.metric_reporter import METRIC_REPORTER
from airflow_monitor.config import AirflowMonitorConfig
from dbnd._core.utils.timezone import utcnow


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


def set_airflow_server_info_started(airflow_server_info):
    airflow_server_info.last_sync_time = utcnow()
    airflow_server_info.monitor_start_time = (
        airflow_server_info.monitor_start_time or airflow_server_info.last_sync_time
    )


def log_fetching_parameters(
    url,
    since,
    dag_ids,
    fetch_quantity,
    fetch_type,
    incomplete_offset,
    include_logs,
    include_task_args,
    include_xcom,
):
    log_message = "Fetching from {} with since={} include_logs={}, include_task_args={}, include_xcom={}, fetch_quantity={}".format(
        url, since, include_logs, include_task_args, include_xcom, fetch_quantity,
    )

    if dag_ids:
        log_message += ", dag_ids={}".format(dag_ids)

    log_message += ", fetch_type={}".format(fetch_type)

    if incomplete_offset is not None:
        log_message += ", incomplete_offset={}".format(incomplete_offset)

    logger.info(log_message)


def dump_unsent_data(data):
    logger.info("Dumping Airflow export data to disk")
    with NamedTemporaryFile(
        mode="w", suffix=".json", prefix="dbnd_export_data_", delete=False
    ) as f:
        json.dump(data, f)
        logger.info("Dumped to %s", f.name)


def save_error_message(airflow_instance_detail, message):
    airflow_instance_detail.airflow_server_info.monitor_error_message = message
    airflow_instance_detail.airflow_server_info.monitor_error_message += "\nTimestamp: {}".format(
        utcnow()
    )
    logging.error(message)
    save_airflow_server_info(airflow_instance_detail.airflow_server_info)


def wait_interval():
    sleep_interval = AirflowMonitorConfig().interval
    logger.info("Waiting for %d seconds", sleep_interval)
    sleep(sleep_interval)
