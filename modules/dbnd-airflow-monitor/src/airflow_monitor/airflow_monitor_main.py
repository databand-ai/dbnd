import logging
import sys
import traceback

from time import sleep

import six

from airflow_monitor.airflow_data_saving import (
    save_airflow_monitor_data,
    save_airflow_server_info,
)
from airflow_monitor.airflow_instance_details import (
    create_airflow_instance_details_with_existing,
    create_instance_details_first_time,
)
from airflow_monitor.airflow_servers_fetching import AirflowServersGetter

from dbnd._core.utils.dotdict import _as_dotted_dict
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor import pendulum


if six.PY3:
    from json import JSONDecodeError
else:
    from simplejson import JSONDecodeError

try:
    import urlparse
except ImportError:
    from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def _log_recieved_tasks(fetched_data):
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


def set_airflow_server_info_started(airflow_server_info):
    airflow_server_info.last_sync_time = utcnow()
    airflow_server_info.monitor_start_time = (
        airflow_server_info.monitor_start_time or airflow_server_info.last_sync_time
    )


def do_fetching_iteration(
    airflow_config, airflow_instance_detail, api_client, tracking_store
):

    try:
        data = airflow_instance_detail.data_fetcher.get_data(
            airflow_instance_detail.since,
            airflow_config.include_logs,
            airflow_config.include_task_args,
            airflow_config.include_xcom,
            airflow_config.dag_ids,
            airflow_config.tasks_per_fetch,
        )

        if data is None:
            logger.warning("Didn't receive any data")
            return

        _log_recieved_tasks(data)

        airflow_instance_detail.tasks_received = len(data.get("task_instances", []))

        export_data = _as_dotted_dict(**data)

        airflow_instance_detail.airflow_server_info.airflow_version = (
            export_data.airflow_version
        )
        airflow_instance_detail.airflow_server_info.dags_path = export_data.dags_path
        airflow_instance_detail.airflow_server_info.logs_path = export_data.logs_path
        airflow_instance_detail.airflow_server_info.airflow_export_version = (
            export_data.airflow_export_version
        )
        airflow_instance_detail.airflow_server_info.synced_from = (
            airflow_instance_detail.airflow_server_info.synced_from
            or airflow_instance_detail.since
        )
        airflow_instance_detail.airflow_server_info.active_dags = {
            dag["dag_id"]: [task["task_id"] for task in dag["tasks"]]
            for dag in export_data.dags
            if dag.get("is_active", True)
        }

        end_dates = [
            pendulum.parse(str(t["end_date"]))
            for t in export_data.task_instances
            if t["end_date"] is not None
        ]
        airflow_instance_detail.airflow_server_info.synced_to = (
            max(end_dates) if end_dates else utcnow()
        )

        save_airflow_monitor_data(
            data,
            tracking_store,
            airflow_instance_detail.airflow_server_info.base_url,
            airflow_instance_detail.airflow_server_info.last_sync_time,
        )

        save_airflow_server_info(
            airflow_instance_detail.airflow_server_info, api_client
        )

        airflow_instance_detail.since = (
            airflow_instance_detail.airflow_server_info.synced_to
        )
    except JSONDecodeError:
        logger.exception("Could not decode the received data, error in json format.")
        (
            exception_type,
            exception,
            airflow_instance_detail.exception_traceback,
        ) = sys.exc_info()
    except (ConnectionError, OSError, IOError) as e:
        logger.error(
            "An error occurred while trying to sync data from airflow to databand: {}".format(
                e
            )
        )
        (
            exception_type,
            exception,
            airflow_instance_detail.exception_traceback,
        ) = sys.exc_info()
    except Exception as e:
        logger.exception(
            "An error occurred while trying to sync data from airflow to databand."
        )
        (
            exception_type,
            exception,
            airflow_instance_detail.exception_traceback,
        ) = sys.exc_info()
    finally:
        if airflow_instance_detail.exception_traceback is not None:
            airflow_instance_detail.airflow_server_info.monitor_error_message = "".join(
                traceback.format_tb(airflow_instance_detail.exception_traceback)
            )
            airflow_instance_detail.airflow_server_info.monitor_error_message += "{}: {}. ".format(
                exception_type.__name__, exception
            )
            airflow_instance_detail.airflow_server_info.monitor_error_message += "Timestamp: {}".format(
                utcnow()
            )
            save_airflow_server_info(
                airflow_instance_detail.airflow_server_info, api_client
            )

            logger.info(
                "Sleeping for {} seconds on error".format(airflow_config.interval)
            )
            sleep(airflow_config.interval)
        airflow_instance_detail.exception_traceback = None


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

    if not existing_airflow_instance_details:
        airflow_instance_details = create_instance_details_first_time(
            monitor_args, airflow_config, configs_fetched, api_client
        )
    else:
        airflow_instance_details = create_airflow_instance_details_with_existing(
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
        do_fetching_iteration(
            airflow_config, airflow_instance_detail, api_client, tracking_store,
        )
        if (
            airflow_instance_detail.tasks_received < airflow_config.tasks_per_fetch
            or airflow_instance_detail.tasks_received == 1
        ):
            break


def sync_all_servers(
    monitor_args, airflow_config, airflow_instance_details, api_client, tracking_store,
):
    details_to_remove = []

    for airflow_instance_detail in airflow_instance_details:
        logger.info("Syncing server {}".format(airflow_instance_detail.config.base_url))

        set_airflow_server_info_started(airflow_instance_detail.airflow_server_info)

        fetch_one_server_until_synced(
            airflow_config, airflow_instance_detail, api_client, tracking_store,
        )

        if monitor_args.history_only:
            logger.info(
                "Finished syncing history of server {}".format(
                    airflow_instance_detail.config.base_url
                )
            )
            details_to_remove.append(airflow_instance_detail)
        else:
            airflow_instance_detail.since = (
                airflow_instance_detail.airflow_server_info.synced_to
            ) or pendulum.datetime.min

    if len(details_to_remove) > 0:
        for detail in details_to_remove:
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
            "rbac_enabled": config.rbac_enabled,
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
