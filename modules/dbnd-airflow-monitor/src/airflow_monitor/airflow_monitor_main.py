import json
import logging
import sys
import traceback

from datetime import timedelta
from json import JSONDecodeError
from tempfile import NamedTemporaryFile
from time import sleep

from airflow_monitor.airflow_data_saving import (
    save_airflow_monitor_data,
    save_airflow_server_info,
)
from airflow_monitor.airflow_instance_details import create_airflow_instance_details
from airflow_monitor.airflow_servers_fetching import AirflowServersGetter
from prometheus_client import Summary

from dbnd._core.utils.dotdict import _as_dotted_dict
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor import pendulum


try:
    import urlparse
except ImportError:
    from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def _log_received_tasks(url, fetched_data):
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


def _send_metrics(airflow_instance_detail, fetched_data):
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


def log_fetching_parameters(url, since, airflow_config, incomplete_offset=None):
    log_message = "Fetching from {} with since={} include_logs={}, include_task_args={}, include_xcom={}, fetch_quantity={}".format(
        url,
        since,
        airflow_config.include_logs,
        airflow_config.include_task_args,
        airflow_config.include_xcom,
        airflow_config.fetch_quantity,
    )

    if airflow_config.dag_ids:
        log_message += ", dag_ids={}".format(airflow_config.dag_ids)

    if incomplete_offset is not None:
        log_message += ", incomplete_offset={}".format(incomplete_offset)

    logger.info(log_message)


def do_fetching_iteration(
    airflow_config, airflow_instance_detail, api_client, tracking_store
):
    """
    Fetch from Airflow webserver, return number of items fetched
    """
    try:
        log_fetching_parameters(
            airflow_instance_detail.url, airflow_instance_detail.since, airflow_config
        )

        data = airflow_instance_detail.data_fetcher.get_data(
            airflow_instance_detail.since,
            airflow_config.include_logs,
            airflow_config.include_task_args,
            airflow_config.include_xcom,
            airflow_config.dag_ids,
            airflow_config.fetch_quantity,
            None,
        )
    except JSONDecodeError:
        logger.exception("Could not decode the received data, error in json format.")
        _send_exception_info(airflow_instance_detail, api_client, airflow_config)
        return 0
    except (ConnectionError, OSError, IOError) as e:
        logger.exception(
            "An error occurred while trying to sync data from airflow to databand: %s",
            e,
        )
        _send_exception_info(airflow_instance_detail, api_client, airflow_config)
        return 0

    if data is None:
        logger.warning("Didn't receive any data")
        return 0

    if "error" in data:
        logger.error("Error in Airflow Export Plugin: \n%s", data["error"])
        _save_error_message(
            airflow_instance_detail, data["error"], api_client, airflow_config
        )
        return 0

    try:
        _log_received_tasks(airflow_instance_detail.url, data)
        _send_metrics(airflow_instance_detail, data)

        export_data = _as_dotted_dict(**data)

        active_dags = {
            dag["dag_id"]: [task["task_id"] for task in dag["tasks"]]
            for dag in export_data.dags
            if dag.get("is_active", True)
        }
        logger.info("Got %d active DAGs", len(active_dags))

        airflow_instance_detail.update_airflow_server(
            airflow_version=export_data.airflow_version,
            dags_path=export_data.dags_path,
            logs_path=export_data.logs_path,
            airflow_export_version=export_data.airflow_export_version,
            synced_from=airflow_instance_detail.airflow_server_info.synced_from
            or airflow_instance_detail.since,
            active_dags=active_dags,
        )

        task_instances_end_dates = [
            pendulum.parse(str(t["end_date"]))
            for t in export_data.task_instances
            if t["end_date"] is not None
        ]

        dag_runs_end_dates = [
            pendulum.parse(str(dr["end_date"]))
            for dr in export_data.dag_runs
            if dr["end_date"] is not None
        ]
        logger.info(
            "Got %d task end dates, the last is %s and got %d dag run end dates, the last is %s",
            len(task_instances_end_dates),
            max(task_instances_end_dates) if task_instances_end_dates else None,
            len(dag_runs_end_dates),
            max(dag_runs_end_dates) if dag_runs_end_dates else None,
        )

        end_dates = (
            task_instances_end_dates if task_instances_end_dates else dag_runs_end_dates
        )

        airflow_instance_detail.airflow_server_info.synced_to = (
            max(end_dates) if end_dates else utcnow()
        )
        logger.info(
            "Using last end date %s, New synced_to date is %s",
            max(end_dates) if end_dates else None,
            airflow_instance_detail.airflow_server_info.synced_to,
        )

        save_airflow_monitor_data(
            data,
            tracking_store,
            airflow_instance_detail.url,
            airflow_instance_detail.airflow_server_info.last_sync_time,
        )

        logging.info(
            "Sending airflow server info: url={}, synced_from={}, synced_to={}, last_sync_time={}".format(
                airflow_instance_detail.airflow_server_info.base_url,
                airflow_instance_detail.airflow_server_info.synced_from,
                airflow_instance_detail.airflow_server_info.synced_to,
                airflow_instance_detail.airflow_server_info.last_sync_time,
            )
        )
        save_airflow_server_info(
            airflow_instance_detail.airflow_server_info, api_client
        )

        # If synced_to was set to utcnow(), keep since as it was
        if end_dates:
            logger.info(
                "Updating since, old value: {}, new value: {}".format(
                    airflow_instance_detail.since,
                    airflow_instance_detail.airflow_server_info.synced_to,
                )
            )
            airflow_instance_detail.since = (
                airflow_instance_detail.airflow_server_info.synced_to
            )
        else:
            logger.info(
                "Keeping since as it was {}".format(airflow_instance_detail.since)
            )

        logger.info(
            "Total {} task instances, {} dag runs, {} dags saved to databand web server".format(
                len(export_data.task_instances),
                len(export_data.dag_runs),
                len(export_data.dags),
            )
        )

        total_fetched = max(len(task_instances_end_dates), len(dag_runs_end_dates))
        return total_fetched
    except Exception as e:
        logger.exception(
            "An error occurred while trying to sync data from airflow to databand: %s",
            e,
        )
        _dump_unsent_data(data)
        _send_exception_info(airflow_instance_detail, api_client, airflow_config)
        return 0


def do_incomplete_data_fetching_iteration(
    airflow_config,
    airflow_instance_detail,
    api_client,
    tracking_store,
    incomplete_offset,
):
    """
    Fetch incomplete data from Airflow web server, return number of items fetched
    """
    exception_type, exception, exception_traceback = None, None, None

    # Max time to look for incomplete data, we do not update this but use pagination instead
    since = utcnow() - timedelta(days=airflow_config.oldest_incomplete_data_in_days)

    try:
        log_fetching_parameters(
            airflow_instance_detail.url, since, airflow_config, incomplete_offset,
        )

        data = airflow_instance_detail.data_fetcher.get_data(
            since,
            airflow_config.include_logs,
            airflow_config.include_task_args,
            airflow_config.include_xcom,
            airflow_config.dag_ids,
            airflow_config.fetch_quantity,
            incomplete_offset,
        )
    except JSONDecodeError:
        logger.exception("Could not decode the received data, error in json format.")
        _send_exception_info(airflow_instance_detail, api_client, airflow_config)
        return 0
    except Exception as e:
        logger.exception(
            "An error occurred while trying to sync data from airflow to databand: %s",
            e,
        )
        _send_exception_info(airflow_instance_detail, api_client, airflow_config)
        return 0

    if data is None:
        logger.warning("Didn't receive any incomplete data")
        return 0

    if "error" in data:
        logger.error("Error in Airflow Export Plugin: \n%s", data["error"])
        _save_error_message(
            airflow_instance_detail, data["error"], api_client, airflow_config
        )
        return 0

    try:
        _log_received_tasks(airflow_instance_detail.url, data)
        _send_metrics(airflow_instance_detail, data)

        export_data = _as_dotted_dict(**data)

        save_airflow_monitor_data(
            data,
            tracking_store,
            airflow_instance_detail.url,
            airflow_instance_detail.airflow_server_info.last_sync_time,
        )

        logger.info(
            "Total {} task instances, {} dag runs, {} dags saved to databand web server".format(
                len(export_data.task_instances),
                len(export_data.dag_runs),
                len(export_data.dags),
            )
        )

        total_fetched = max(len(data["task_instances"]), len(data["dag_runs"]))
        return total_fetched
    except Exception as e:
        logger.exception(
            "An error occurred while trying to sync data from airflow to databand: %s",
            e,
        )
        _dump_unsent_data(data)
        _send_exception_info(airflow_instance_detail, api_client, airflow_config)
        return 0


def _dump_unsent_data(data):
    logger.info("Dumping Airflow export data to disk")
    with NamedTemporaryFile(
        mode="w", suffix=".json", prefix="dbnd_export_data_", delete=False
    ) as f:
        json.dump(data, f)
        logger.info("Dumped to %s", f.name)


def _send_exception_info(airflow_instance_detail, api_client, airflow_config):
    exception_type, exception, exception_traceback = sys.exc_info()
    message = "".join(traceback.format_tb(exception_traceback))
    message += "{}: {}. ".format(exception_type.__name__, exception)
    _save_error_message(airflow_instance_detail, message, api_client, airflow_config)


def _save_error_message(airflow_instance_detail, message, api_client, airflow_config):
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
    save_airflow_server_info(airflow_instance_detail.airflow_server_info, api_client)

    logger.info("Sleeping for {} seconds on error".format(airflow_config.interval))
    sleep(airflow_config.interval)


def create_instance_details(
    monitor_args,
    airflow_config,
    api_client,
    configs_fetched,
    existing_airflow_instance_details,
):
    if configs_fetched is None:
        if existing_airflow_instance_details:
            return existing_airflow_instance_details
        return None

    airflow_instance_details = create_airflow_instance_details(
        monitor_args,
        airflow_config,
        api_client,
        configs_fetched,
        existing_airflow_instance_details,
    )

    return airflow_instance_details


def fetch_one_server_until_synced(
    airflow_config, airflow_instance_detail, api_client, tracking_store
):
    # Fetch continuously until we are up to date
    logger.info(
        "Starting to fetch data from %s",
        airflow_instance_detail.airflow_server_info.base_url,
    )

    while True:
        logger.info(
            "Starting sync iteration for {}".format(airflow_instance_detail.url)
        )
        fetch_count = do_fetching_iteration(
            airflow_config, airflow_instance_detail, api_client, tracking_store,
        )
        if fetch_count < airflow_config.fetch_quantity:
            logger.info(
                "Finished syncing completed tasks from %s",
                airflow_instance_detail.airflow_server_info.base_url,
            )
            break
        logger.info(
            "Finished sync iteration for {}".format(airflow_instance_detail.url)
        )

    # Fetch separately incomplete data
    logger.info(
        "Starting to fetch incomplete data from %s",
        airflow_instance_detail.airflow_server_info.base_url,
    )
    incomplete_offset = 0
    while True:
        logger.info(
            "Starting sync iteration of incomplete data for {} with incomplete offset {}-{}".format(
                airflow_instance_detail.url,
                incomplete_offset,
                incomplete_offset + airflow_config.fetch_quantity,
            )
        )
        fetch_count = do_incomplete_data_fetching_iteration(
            airflow_config,
            airflow_instance_detail,
            api_client,
            tracking_store,
            incomplete_offset,
        )
        if fetch_count < airflow_config.fetch_quantity:
            logger.info(
                "Finished syncing incomplete data from {} after receiving {} items which is less then fetch quantity {}".format(
                    airflow_instance_detail.airflow_server_info.base_url,
                    fetch_count,
                    airflow_config.fetch_quantity,
                )
            )
            break
        else:
            incomplete_offset += airflow_config.fetch_quantity
            logger.info(
                "Fetched incomplete data from %s, new incomplete offset: %d",
                airflow_instance_detail.airflow_server_info.base_url,
                incomplete_offset,
            )
        logger.info(
            "Finished sync iteration of incomplete data for {}".format(
                airflow_instance_detail.url
            )
        )


def sync_all_servers(
    monitor_args, airflow_config, airflow_instance_details, api_client, tracking_store,
):
    details_to_remove = []

    for airflow_instance_detail in airflow_instance_details:
        logger.info("Starting to sync server {}".format(airflow_instance_detail.url))

        set_airflow_server_info_started(airflow_instance_detail.airflow_server_info)

        fetch_one_server_until_synced(
            airflow_config, airflow_instance_detail, api_client, tracking_store,
        )

        if monitor_args.history_only:
            logger.info(
                "Finished syncing history of server {}".format(
                    airflow_instance_detail.url
                )
            )
            details_to_remove.append(airflow_instance_detail)

    if len(details_to_remove) > 0:
        for detail in details_to_remove:
            airflow_instance_details.remove(detail)

    logger.info(
        "Completed servers {} ".format(
            ",".join([detail.url for detail in details_to_remove])
        )
    )
    logger.info(
        "Remaining servers {} ".format(
            ",".join([detail.url for detail in airflow_instance_details])
        )
    )


def airflow_monitor_main(monitor_args, airflow_config):
    """Start Airflow Data Importer"""
    from dbnd import new_dbnd_context

    airflow_instance_details = []

    with new_dbnd_context() as dc:
        databand_url = dc.settings.core.databand_url
        api_client = dc.databand_api_client
        iteration_number = 0

        servers_fetcher = AirflowServersGetter(databand_url, api_client)

        while True:
            configs_fetched = servers_fetcher.get_fetching_configuration(airflow_config)
            airflow_instance_details = create_instance_details(
                monitor_args,
                airflow_config,
                api_client,
                configs_fetched,
                airflow_instance_details,
            )

            if airflow_instance_details is None:
                # Probably an error occurred
                logger.info(
                    "Sleeping {} seconds on error".format(airflow_config.interval)
                )
                sleep(airflow_config.interval)
                continue

            sync_all_servers(
                monitor_args,
                airflow_config,
                airflow_instance_details,
                api_client,
                dc.tracking_store_allow_errors,
            )

            # We are running in history_only mode and finished syncing all servers
            if monitor_args.history_only and len(airflow_instance_details) == 0:
                logger.info("Finished syncing all servers")
                break

            # Limit the total number of sync iterations, mostly for tests and debug
            iteration_number += 1
            if monitor_args.number_of_iterations is not None:
                if iteration_number >= monitor_args.number_of_iterations:
                    logger.info(
                        "We are done with all required %s iterations", iteration_number
                    )
                    break

            # Give servers some time to get filled with data
            logger.info("Waiting for %d seconds", airflow_config.interval)
            sleep(airflow_config.interval)
