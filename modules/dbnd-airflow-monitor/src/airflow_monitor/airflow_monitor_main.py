import logging

from datetime import timedelta
from time import sleep

from airflow_monitor.airflow_data_saving import (
    save_airflow_monitor_data,
    save_airflow_server_info,
)
from airflow_monitor.airflow_instance_details import create_instance_details_list
from airflow_monitor.airflow_monitor_utils import (
    dump_unsent_data,
    log_fetching_parameters,
    log_received_tasks,
    save_error_message,
    send_metrics,
    set_airflow_server_info_started,
    wait_interval,
)
from airflow_monitor.airflow_servers_fetching import AirflowServersGetter
from airflow_monitor.errors import AirflowFetchingException
from dbnd._core.errors.friendly_error.tools import logger_format_for_databand_error
from dbnd._core.utils.dotdict import _as_dotted_dict
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor import pendulum


logger = logging.getLogger(__name__)


# Fetch types
COMPLETE = "complete"
INCOMPLETE_TYPE1 = "incomplete_type1"
INCOMPLETE_TYPE2 = "incomplete_type2"
DAGS_ONLY = "dags_only"


def try_fetching_from_airflow(
    airflow_instance_detail, since, fetch_type, incomplete_offset
):
    """
    Try fetching from Airflow and return the data that was fetched.
    If Exception occurred, return None.
    """
    try:
        log_fetching_parameters(
            airflow_instance_detail.url,
            since,
            airflow_instance_detail.config.dag_ids,
            airflow_instance_detail.config.fetch_quantity,
            fetch_type,
            incomplete_offset,
            airflow_instance_detail.config.include_logs,
            airflow_instance_detail.config.include_task_args,
            airflow_instance_detail.config.include_xcom,
        )

        data = airflow_instance_detail.data_fetcher.get_data(
            since,
            airflow_instance_detail.config.include_logs,
            airflow_instance_detail.config.include_task_args,
            airflow_instance_detail.config.include_xcom,
            airflow_instance_detail.config.dag_ids,
            airflow_instance_detail.config.fetch_quantity,
            fetch_type,
            incomplete_offset,
        )
        return data
    except AirflowFetchingException as afe:
        message = logger_format_for_databand_error(afe)
        save_error_message(airflow_instance_detail, message)
        return None
    except Exception as e:
        message = "Exception occurred while trying to fetch data from Airflow url {}. Exception: {}".format(
            e, airflow_instance_detail.url
        )
        save_error_message(airflow_instance_detail, message)
        return None


def get_active_dags(export_data):
    active_dags = {
        dag["dag_id"]: [task["task_id"] for task in dag["tasks"]]
        for dag in export_data.dags
        if dag.get("is_active", True)
    }

    return active_dags


def get_max_end_dates(export_data):
    """
    Return the max end date from all task instances.
    If there are no task instances, return max end date from all dag_runs.
    If there are no dag runs as well, return None.
    """
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

    if not end_dates:
        return None

    return max(end_dates)


def update_since(airflow_instance_detail, max_end_date, is_incomplete):
    if not max_end_date:
        if is_incomplete:
            logger.info(
                "Keeping incomplete since as it was %s",
                airflow_instance_detail.incomplete_since,
            )
        else:
            logger.info("Keeping since as it was %s", airflow_instance_detail.since)
        return

    if is_incomplete:
        logger.info(
            "Updating incomplete since, old value: %s, new value: %s",
            airflow_instance_detail.incomplete_since,
            max_end_date,
        )
        airflow_instance_detail.incomplete_since = max_end_date
    else:
        logger.info(
            "Updating since, old value: %s, new value: %s",
            airflow_instance_detail.since,
            max_end_date,
        )
        airflow_instance_detail.since = max_end_date


def process_and_update_airflow_server_info(
    airflow_instance_detail, export_data, max_end_date, is_incomplete
):
    active_dags = get_active_dags(export_data)

    airflow_instance_detail.update_airflow_server(
        airflow_version=export_data.airflow_version,
        dags_path=export_data.dags_path,
        logs_path=export_data.logs_path,
        airflow_export_version=export_data.airflow_export_version,
        synced_from=airflow_instance_detail.airflow_server_info.synced_from
        or airflow_instance_detail.since,
        active_dags=active_dags,
    )

    if is_incomplete:
        airflow_instance_detail.airflow_server_info.incomplete_synced_to = (
            max_end_date if max_end_date else utcnow()
        )
    else:
        airflow_instance_detail.airflow_server_info.synced_to = (
            max_end_date if max_end_date else utcnow()
        )

    synced_to_name = "incomplete_synced_to" if is_incomplete else "synced_to"
    synced_to = (
        airflow_instance_detail.airflow_server_info.incomplete_synced_to
        if is_incomplete
        else airflow_instance_detail.airflow_server_info.synced_to
    )

    logger.info(
        "Using last end date %s, New %s date is %s",
        max_end_date if max_end_date else None,
        synced_to_name,
        str(synced_to),
    )

    logging.info(
        "Sending airflow server info: url=%s, synced_from=%s, %s=%s, last_sync_time=%s",
        airflow_instance_detail.airflow_server_info.base_url,
        airflow_instance_detail.airflow_server_info.synced_from,
        synced_to_name,
        str(synced_to),
        airflow_instance_detail.airflow_server_info.last_sync_time,
    )
    save_airflow_server_info(airflow_instance_detail.airflow_server_info)


def do_data_fetching_iteration(
    since, fetch_type, incomplete_offset, airflow_instance_detail
):
    """
    This function performs one fetching iteration from Airflow webserver to Databand.
    This iteration can fetch complete/incomplete according to the parameters given.
    It will fetch from Airflow, send to Databand and return the fetched data
    """
    logger.info(
        "Running sync iteration with since=%s, fetch_type=%s, incomplete_offset=%s",
        since,
        fetch_type,
        incomplete_offset,
    )

    data = try_fetching_from_airflow(
        airflow_instance_detail=airflow_instance_detail,
        since=since,
        fetch_type=fetch_type,
        incomplete_offset=incomplete_offset,
    )

    if data is None:
        logger.warning("Didn't receive any data, probably an error occurred")
        return None

    if "error" in data:
        logger.error("Error in Airflow Export Plugin: \n%s", data["error"])
        save_error_message(airflow_instance_detail, data["error"])
        return None

    log_received_tasks(airflow_instance_detail.url, data)
    send_metrics(airflow_instance_detail.airflow_server_info.base_url, data)

    try:
        save_airflow_monitor_data(
            data,
            airflow_instance_detail.url,
            airflow_instance_detail.airflow_server_info.last_sync_time,
        )

        logger.info(
            "Total %s task instances, %s dag runs, %s dags saved to databand web server",
            len(data["task_instances"]),
            len(data["dag_runs"]),
            len(data["dags"]),
        )

        export_data = _as_dotted_dict(**data)
        return export_data
    except Exception as e:
        logger.exception(
            "An error occurred while trying to sync data from airflow to databand: %s",
            e,
        )
        dump_unsent_data(data)
        save_error_message(airflow_instance_detail, e)
        return None


def sync_dags_only(airflow_instance_detail):
    """
    Sync only dags from Airflow in their raw form.
    """
    logger.info(
        "Syncing dags list from %s",
        airflow_instance_detail.airflow_server_info.base_url,
    )

    do_data_fetching_iteration(
        since=None,
        fetch_type=DAGS_ONLY,
        incomplete_offset=None,
        airflow_instance_detail=airflow_instance_detail,
    )


def sync_all_complete_data(airflow_instance_detail):
    """
    Sync all completed data (task instances and dag runs).
    If the returned amount of data is less than fetch_quantity, we can stop.
    """
    logger.info("Starting to sync complete data from %s", airflow_instance_detail.url)

    while True:
        export_data = do_data_fetching_iteration(
            since=airflow_instance_detail.since,
            fetch_type=COMPLETE,
            incomplete_offset=None,
            airflow_instance_detail=airflow_instance_detail,
        )
        if export_data is None:
            return

        max_end_date = get_max_end_dates(export_data)
        process_and_update_airflow_server_info(
            airflow_instance_detail, export_data, max_end_date, False
        )
        update_since(airflow_instance_detail, max_end_date, False)
        fetch_count = max(len(export_data.task_instances), len(export_data.dag_runs))

        if fetch_count < airflow_instance_detail.config.fetch_quantity:
            break

    logger.info(
        "Finished syncing complete data from %s",
        airflow_instance_detail.airflow_server_info.base_url,
    )


def sync_all_incomplete_data_type1(airflow_instance_detail):
    """
    Fetch all incomplete task instances from completed dag runs.
    If the returned amount of data is less than fetch_quantity, we can stop.
    """
    logger.info(
        "Starting to sync incomplete data from complete dag runs for %s",
        airflow_instance_detail.url,
    )

    offset = 0
    last_dag_run_in_previous_iteration = None

    while True:
        export_data = do_data_fetching_iteration(
            since=airflow_instance_detail.incomplete_since,
            fetch_type=INCOMPLETE_TYPE1,
            incomplete_offset=offset,
            airflow_instance_detail=airflow_instance_detail,
        )
        if export_data is None:
            return

        fetch_count = len(export_data.task_instances)

        # No more to fetch, we can stop now
        if fetch_count < airflow_instance_detail.config.fetch_quantity:
            if len(export_data.dag_runs) != 0:
                new_since = pendulum.parse(str(export_data.dag_runs[0]["end_date"]))
                update_since(airflow_instance_detail, new_since, True)
                process_and_update_airflow_server_info(
                    airflow_instance_detail, export_data, new_since, True
                )
            else:
                if last_dag_run_in_previous_iteration:
                    new_since = pendulum.parse(
                        str(last_dag_run_in_previous_iteration["end_date"])
                    )
                    update_since(airflow_instance_detail, new_since, True)
                    process_and_update_airflow_server_info(
                        airflow_instance_detail, export_data, new_since, True
                    )
            break

        export_data.dag_runs.sort(key=lambda x: pendulum.parse(str(x["end_date"])))

        last_full_dag_run = (
            export_data.dag_runs[-2] if len(export_data.dag_runs) >= 2 else None
        )
        last_dag_run = export_data.dag_runs[-1]

        number_of_task_instances_in_last_dag_run = len(
            [
                task_instance
                for task_instance in export_data.task_instances
                if task_instance["dag_id"] == last_dag_run["dag_id"]
                and task_instance["execution_date"] == last_dag_run["execution_date"]
            ]
        )

        # Here we assume that we received all task instances from all dag runs except for the last one.
        # The last run may be full but it may be not.
        if last_full_dag_run:
            # Bump the since according to the last full dag run and change the offset to be the number of task instances
            # that we received in the last dag run
            max_end_date = pendulum.parse(str(last_full_dag_run["end_date"]))
            update_since(airflow_instance_detail, max_end_date, True)
            offset = number_of_task_instances_in_last_dag_run
            process_and_update_airflow_server_info(
                airflow_instance_detail, export_data, max_end_date, True
            )
        else:
            if last_dag_run_in_previous_iteration and (
                last_dag_run_in_previous_iteration["dag_id"] != last_dag_run["dag_id"]
                or last_dag_run_in_previous_iteration["execution_date"]
                != last_dag_run["execution_date"]
            ):
                # We received one run which is different than the previous one, bump the since and change the offset
                max_end_date = pendulum.parse(
                    str(last_dag_run_in_previous_iteration["end_date"])
                )
                update_since(airflow_instance_detail, max_end_date, True)
                offset = number_of_task_instances_in_last_dag_run
                process_and_update_airflow_server_info(
                    airflow_instance_detail, export_data, max_end_date, True
                )
            else:
                # We received only one run and don't know if its full - keep the since and bump the offset
                offset += number_of_task_instances_in_last_dag_run

        last_dag_run_in_previous_iteration = last_dag_run

    logger.info(
        "Finished syncing incomplete data from complete dag runs for %s",
        airflow_instance_detail.url,
    )


def sync_all_incomplete_data_type2(airflow_instance_detail):
    """
    Sync all incomplete task instances (from incomplete dag runs) and incomplete dag runs.
    Use offset to advance instead of since (because it uses pagination).
    If the returned amount of data is less than fetch_quantity, we can stop.
    """
    logger.info(
        "Starting to sync incomplete from incomplete dag runs for %s",
        airflow_instance_detail.url,
    )

    incomplete_offset = 0
    # Max time to look for incomplete data, we do not update this but use pagination instead
    since = utcnow() - timedelta(
        days=airflow_instance_detail.config.oldest_incomplete_data_in_days
    )

    while True:
        logger.info(
            "Starting sync iteration of incomplete data for %s with incomplete offset %s-%s",
            airflow_instance_detail.url,
            incomplete_offset,
            incomplete_offset + airflow_instance_detail.config.fetch_quantity,
        )
        export_data = do_data_fetching_iteration(
            since=since,
            fetch_type=INCOMPLETE_TYPE2,
            incomplete_offset=incomplete_offset,
            airflow_instance_detail=airflow_instance_detail,
        )
        if export_data is None:
            return

        max_end_date = get_max_end_dates(export_data)
        update_since(airflow_instance_detail, max_end_date, True)

        fetch_count = max(len(export_data.task_instances), len(export_data.dag_runs))

        if fetch_count < airflow_instance_detail.config.fetch_quantity:
            break
        else:
            incomplete_offset += airflow_instance_detail.config.fetch_quantity
            logger.info(
                "Fetched incomplete data from %s, new incomplete offset: %d",
                airflow_instance_detail.airflow_server_info.base_url,
                incomplete_offset,
            )

    logger.info(
        "Finished syncing incomplete data from %s",
        airflow_instance_detail.airflow_server_info.base_url,
    )


def fetch_one_server_until_synced(airflow_instance_detail):
    """
    Fetch continuously all types of data from one server until we are up to date.
    """
    # Sync all dags first
    sync_dags_only(airflow_instance_detail)

    # Sync all completed
    sync_all_complete_data(airflow_instance_detail)

    # Sync all completed task instances from completed dag runs (and their dag runs).
    # Use the original since that we started with and not the one after we finished syncing all complete data.
    sync_all_incomplete_data_type1(airflow_instance_detail)

    # Sync all other incomplete data (incomplete task instances from incomplete dag runs)
    # that can't be synced in any other way - using pagination
    sync_all_incomplete_data_type2(airflow_instance_detail)


def sync_all_servers(monitor_args, airflow_instance_details):
    details_to_remove = []

    for airflow_instance_detail in airflow_instance_details:
        logger.info("Starting to sync server %s", airflow_instance_detail.url)

        set_airflow_server_info_started(airflow_instance_detail.airflow_server_info)

        fetch_one_server_until_synced(airflow_instance_detail)

        logger.info("Finished syncing server %s", airflow_instance_detail.url)

        if monitor_args.history_only:
            logger.info(
                "Finished syncing history of server %s", airflow_instance_detail.url
            )
            details_to_remove.append(airflow_instance_detail)

    if len(details_to_remove) > 0:
        for detail in details_to_remove:
            airflow_instance_details.remove(detail)

    logger.info(
        "Completed servers %s ", ",".join([detail.url for detail in details_to_remove])
    )
    logger.info(
        "Remaining servers %s ",
        ",".join([detail.url for detail in airflow_instance_details]),
    )


def airflow_monitor_main(monitor_args):
    airflow_instance_details = []
    iteration_number = 0
    servers_fetcher = AirflowServersGetter()

    while True:
        configs_fetched = servers_fetcher.get_fetching_configurations()

        if configs_fetched is not None:
            airflow_instance_details = create_instance_details_list(
                monitor_args, configs_fetched, airflow_instance_details,
            )

            sync_all_servers(monitor_args, airflow_instance_details)

            # We are running in history_only mode and finished syncing all servers
            if monitor_args.history_only and len(airflow_instance_details) == 0:
                logger.info("Finished syncing all servers")
                break

        # Limit the total number of sync iterations, mostly for tests and debug
        # used in monitor-as-dag
        iteration_number += 1
        if monitor_args.number_of_iterations is not None:
            if iteration_number >= monitor_args.number_of_iterations:
                logger.info(
                    "We are done with all required %d iterations", iteration_number
                )
                break

        wait_interval()
