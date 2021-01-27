import logging

from datetime import timedelta
from json import JSONDecodeError
from time import sleep

from airflow_monitor.airflow_data_saving import (
    save_airflow_monitor_data,
    save_airflow_server_info,
)
from airflow_monitor.airflow_instance_details import create_instance_details
from airflow_monitor.airflow_monitor_utils import (
    dump_unsent_data,
    log_fetching_parameters,
    log_received_tasks,
    save_error_message,
    send_exception_info,
    send_metrics,
    set_airflow_server_info_started,
)
from airflow_monitor.airflow_servers_fetching import AirflowServersGetter

from dbnd._core.utils.dotdict import _as_dotted_dict
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor import pendulum


try:
    import urlparse
except ImportError:
    from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def try_fetching_from_airflow(
    airflow_instance_detail,
    airflow_config,
    since,
    incomplete_offset,
    api_client,
    dags_only,
):
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
            dags_only,
        )
        return data
    except JSONDecodeError:
        logger.exception("Could not decode the received data, error in json format.")
        send_exception_info(airflow_instance_detail, api_client, airflow_config)
    except (ConnectionError, OSError, IOError) as e:
        logger.exception(
            "An error occurred while trying to sync data from airflow to databand: %s",
            e,
        )
        send_exception_info(airflow_instance_detail, api_client, airflow_config)
    return None


def validate_airflow_monitor_data(
    data, airflow_instance_detail, airflow_config, api_client
):
    if data is None:
        logger.warning("Didn't receive any data")
        return False

    if "error" in data:
        logger.error("Error in Airflow Export Plugin: \n%s", data["error"])
        save_error_message(
            airflow_instance_detail, data["error"], api_client, airflow_config
        )
        return False
    return True


def do_fetching_iteration(
    airflow_config, airflow_instance_detail, api_client, tracking_store
):
    """
    Fetch from Airflow webserver, return number of items fetched
    """
    data = try_fetching_from_airflow(
        airflow_instance_detail,
        airflow_config,
        airflow_instance_detail.since,
        None,
        api_client,
        False,
    )

    if not validate_airflow_monitor_data(
        data, airflow_instance_detail, airflow_config, api_client
    ):
        return 0

    try:
        log_received_tasks(airflow_instance_detail.url, data)
        send_metrics(airflow_instance_detail, data)

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
        dump_unsent_data(data)
        send_exception_info(airflow_instance_detail, api_client, airflow_config)
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
    # Max time to look for incomplete data, we do not update this but use pagination instead
    since = utcnow() - timedelta(days=airflow_config.oldest_incomplete_data_in_days)

    data = try_fetching_from_airflow(
        airflow_instance_detail,
        airflow_config,
        since,
        incomplete_offset,
        api_client,
        False,
    )

    if not validate_airflow_monitor_data(
        data, airflow_instance_detail, airflow_config, api_client
    ):
        return 0

    try:
        log_received_tasks(airflow_instance_detail.url, data)
        send_metrics(airflow_instance_detail, data)

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
        dump_unsent_data(data)
        send_exception_info(airflow_instance_detail, api_client, airflow_config)
        return 0


def fetch_dags_only(
    airflow_instance_detail, airflow_config, api_client, tracking_store
):
    logger.info(
        "Bringing dags list from {}".format(
            airflow_instance_detail.airflow_server_info.base_url
        )
    )
    data = airflow_instance_detail.data_fetcher.get_data(
        airflow_instance_detail.since,
        airflow_config.include_logs,
        airflow_config.include_task_args,
        airflow_config.include_xcom,
        airflow_config.dag_ids,
        airflow_config.fetch_quantity,
        None,
        True,
    )

    try:
        logger.info(
            "Received data from %s with: {dags: %d}",
            airflow_instance_detail.url,
            len(data["dags"]),
        )
        save_airflow_monitor_data(
            data,
            tracking_store,
            airflow_instance_detail.url,
            airflow_instance_detail.airflow_server_info.last_sync_time,
        )
        logger.info(
            "Total {} dags saved to databand web server".format(len(data["dags"]))
        )
    except Exception as e:
        logger.exception(
            "An error occurred while trying to sync data from airflow to databand: %s",
            e,
        )
        dump_unsent_data(data)
        send_exception_info(airflow_instance_detail, api_client, airflow_config)
        return 0


def fetch_one_server_until_synced(
    airflow_config, airflow_instance_detail, api_client, tracking_store
):

    # Fetch continuously until we are up to date
    logger.info(
        "Starting to fetch data from %s",
        airflow_instance_detail.airflow_server_info.base_url,
    )

    fetch_dags_only(airflow_instance_detail, airflow_config, api_client, tracking_store)

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
