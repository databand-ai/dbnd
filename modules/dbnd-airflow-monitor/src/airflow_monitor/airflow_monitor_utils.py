import json
import logging
import sys
import traceback

from tempfile import NamedTemporaryFile
from time import sleep

from airflow_monitor.airflow_data_saving import save_airflow_server_info
from prometheus_client import Summary

from dbnd._core.utils.timezone import utcnow


logger = logging.getLogger(__name__)


def log_received_tasks(url, fetched_data):
    try:
        logger.info(
            "Received data from %s with: {tasks: %d, dags: %d, dag_runs: %d, since: %s}",
            url,
            len(fetched_data.get("task_instances", [])),
            len(fetched_data.get("dags", [])),
            len(fetched_data.get("dag_runs", [])),
            fetched_data.get("since"),
        )
    except Exception as e:
        logging.error("Could not log received data. %s", e)


prometheus_metrics = {
    "performance": Summary(
        "dbnd_af_plugin_query_duration_seconds",
        "Airflow Export Plugin Query Run Time",
        ["airflow_instance", "method_name"],
    ),
    "sizes": Summary(
        "dbnd_af_plugin_query_result_size",
        "Airflow Export Plugin Query Result Size",
        ["airflow_instance", "method_name"],
    ),
}


def send_metrics(airflow_instance_detail, fetched_data):
    try:
        metrics = fetched_data.get("metrics")
        logger.info("Received Grafana Metrics from airflow plugin: %s", metrics)
        for key, metrics_dict in metrics.items():
            for metric_name, value in metrics_dict.items():
                prometheus_metrics[key].labels(
                    airflow_instance_detail.airflow_server_info.base_url,
                    metric_name.lstrip("_"),
                ).observe(value)
    except Exception as e:
        logger.error("Failed to send plugin metrics. %s", e)


def set_airflow_server_info_started(airflow_server_info):
    airflow_server_info.last_sync_time = utcnow()
    airflow_server_info.monitor_start_time = (
        airflow_server_info.monitor_start_time or airflow_server_info.last_sync_time
    )


def log_fetching_parameters(
    url, since, airflow_config, fetch_quantity, fetch_type, incomplete_offset
):
    log_message = "Fetching from {} with since={} include_logs={}, include_task_args={}, include_xcom={}, fetch_quantity={}".format(
        url,
        since,
        airflow_config.include_logs,
        airflow_config.include_task_args,
        airflow_config.include_xcom,
        fetch_quantity,
    )

    if airflow_config.dag_ids:
        log_message += ", dag_ids={}".format(airflow_config.dag_ids)

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


def send_exception_info(airflow_instance_detail, airflow_config):
    exception_type, exception, exception_traceback = sys.exc_info()
    message = "".join(traceback.format_tb(exception_traceback))
    message += "{}: {}. ".format(exception_type.__name__, exception)
    save_error_message(airflow_instance_detail, message, airflow_config)


def save_error_message(airflow_instance_detail, message, airflow_config):
    airflow_instance_detail.airflow_server_info.monitor_error_message = message
    airflow_instance_detail.airflow_server_info.monitor_error_message += "Timestamp: {}".format(
        utcnow()
    )
    logging.info(
        "Sending airflow server info: url={}, error={}".format(
            airflow_instance_detail.airflow_server_info.base_url,
            airflow_instance_detail.airflow_server_info.monitor_error_message,
        )
    )
    save_airflow_server_info(airflow_instance_detail.airflow_server_info)

    logger.info("Sleeping for {} seconds on error".format(airflow_config.interval))
    sleep(airflow_config.interval)
