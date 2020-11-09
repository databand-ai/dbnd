import logging
import sys
import traceback

from datetime import timedelta
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


def _log_received_tasks(fetched_data):
    try:
        logger.info(
            "Parsing received data with: {tasks: %d, dags: %d, dag_runs: %d, since: %s}",
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
        logger.info("Metrics from airflow plugin: %s", metrics)
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
    exception_type, exception, exception_traceback = None, None, None

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

        if data is None:
            logger.warning("Didn't receive any data")
            return 0

        if "error" in data:
            logger.error("Error in Airflow Export Plugin: \n%s", data["error"])
            _save_error_message(
                airflow_instance_detail, data["error"], api_client, airflow_config
            )
            return 0

        _log_received_tasks(data)
        _send_metrics(airflow_instance_detail, data)

        export_data = _as_dotted_dict(**data)

        active_dags = {
            dag["dag_id"]: [task["task_id"] for task in dag["tasks"]]
            for dag in export_data.dags
            if dag.get("is_active", True)
        }
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

        end_dates = (
            task_instances_end_dates if task_instances_end_dates else dag_runs_end_dates
        )

        airflow_instance_detail.airflow_server_info.synced_to = (
            max(end_dates) if end_dates else utcnow()
        )

        save_airflow_monitor_data(
            data,
            tracking_store,
            airflow_instance_detail.url,
            airflow_instance_detail.airflow_server_info.last_sync_time,
        )

        save_airflow_server_info(
            airflow_instance_detail.airflow_server_info, api_client
        )

        # If synced_to was set to utcnow(), keep since as it was
        if end_dates:
            airflow_instance_detail.since = (
                airflow_instance_detail.airflow_server_info.synced_to
            )
            logger.info("Setting since to be {}".format(airflow_instance_detail.since))
        else:
            logger.info(
                "Keeping since as it was {}".format(airflow_instance_detail.since)
            )

        # We don't count those who have end_date=None because there can be many of them - more than the limit, also
        # the plugin will return them every time, so we don't want think we are fetching new stuff when it's really not
        total_fetched = max(len(task_instances_end_dates), len(dag_runs_end_dates))
        logger.info("Total {} items fetched".format(total_fetched))
        return total_fetched
    except Exception as e:
        logger.error(
            "An error occurred while trying to sync data from airflow to databand: {}".format(
                e
            )
        )
        exception_type, exception, exception_traceback = sys.exc_info()
    finally:
        if exception_traceback is not None:
            message = "".join(traceback.format_tb(exception_traceback))
            message += "{}: {}. ".format(exception_type.__name__, exception)
            _save_error_message(
                airflow_instance_detail, message, api_client, airflow_config
            )
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

        if data is None:
            logger.warning("Didn't receive any incomplete data")
            return 0

        if "error" in data:
            logger.error("Error in Airflow Export Plugin: \n%s", data["error"])
            _save_error_message(
                airflow_instance_detail, data["error"], api_client, airflow_config
            )
            return 0

        _log_received_tasks(data)
        _send_metrics(airflow_instance_detail, data)

        save_airflow_monitor_data(
            data,
            tracking_store,
            airflow_instance_detail.url,
            airflow_instance_detail.airflow_server_info.last_sync_time,
        )

        total_fetched = max(len(data["task_instances"]), len(data["dag_runs"]))
        logger.info("Total {} items fetched".format(total_fetched))
        return total_fetched
    except Exception as e:
        logger.error(
            "An error occurred while trying to sync data from airflow to databand: {}".format(
                e
            )
        )
        exception_type, exception, exception_traceback = sys.exc_info()
    finally:
        if exception_traceback is not None:
            message = "".join(traceback.format_tb(exception_traceback))
            message += "{}: {}. ".format(exception_type.__name__, exception)
            _save_error_message(
                airflow_instance_detail, message, api_client, airflow_config
            )
            return 0


def _save_error_message(airflow_instance_detail, message, api_client, airflow_config):
    airflow_instance_detail.airflow_server_info.monitor_error_message = message
    airflow_instance_detail.airflow_server_info.monitor_error_message += "Timestamp: {}".format(
        utcnow()
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
    while True:
        fetch_count = do_fetching_iteration(
            airflow_config, airflow_instance_detail, api_client, tracking_store,
        )
        if fetch_count < airflow_config.fetch_quantity:
            break

    # Fetch separately incomplete data
    incomplete_offset = 0
    while True:
        fetch_count = do_incomplete_data_fetching_iteration(
            airflow_config,
            airflow_instance_detail,
            api_client,
            tracking_store,
            incomplete_offset,
        )
        if fetch_count < airflow_config.fetch_quantity:
            break
        else:
            incomplete_offset += airflow_config.fetch_quantity


def sync_all_servers(
    monitor_args, airflow_config, airflow_instance_details, api_client, tracking_store,
):
    details_to_remove = []

    for airflow_instance_detail in airflow_instance_details:
        logger.info("Syncing server {}".format(airflow_instance_detail.url))

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
            logger.info("Removing server {} from fetching list".format(detail.url))
            airflow_instance_details.remove(detail)


def update_airflow_servers_with_local_config(api_client, config):
    """
    Update web server with the locally configured server
    There can be several servers with source=config, but only one with source=config and is_sync_enabled=True
    Therefore, if we restart monitor with different config url, the previous one will be set to is_sync_enabled=False
    If user changes (via UI) is_sync_enabled to True, it will automatically change to source=db
    """
    external_url = (
        config.airflow_external_url
        if config.airflow_external_url
        else config.airflow_url
    )
    api_client.api_request(
        "airflow_web_servers/add_server",
        {},
        "GET",
        query={
            "url": config.airflow_url,
            "is_sync_enabled": True,
            "fetcher": config.fetcher,
            "composer_client_id": config.composer_client_id,
            "api_mode": config.api_mode,
            "source": "config",
            "external_url": external_url,
        },
    )


def airflow_monitor_main(monitor_args, airflow_config):
    """Start Airflow Data Importer"""
    from dbnd import new_dbnd_context

    airflow_instance_details = []

    with new_dbnd_context() as dc:
        databand_url = dc.settings.core.databand_url
        api_client = dc.databand_api_client
        iteration_number = 0

        try:
            # we have implicit definition of airflow server (to sync with)
            # in the config file/env
            # we push it into the databand ,and fetch it back at get_fetching_configuration
            if airflow_config.airflow_url:
                update_airflow_servers_with_local_config(
                    dc.databand_api_client, airflow_config
                )
            else:
                logger.warning(
                    "No airflow url was set in your local airflow monitor configuration."
                )
        except Exception as e:
            logger.error(
                "Could not update list of Airflow servers in the Databand server with local config. Error:{}".format(
                    e
                )
            )
            return

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
            if len(airflow_instance_details) == 0:
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
