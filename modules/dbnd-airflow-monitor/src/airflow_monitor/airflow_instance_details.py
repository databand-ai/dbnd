import logging

from datetime import timedelta

import pkg_resources
import pytz

from airflow_monitor.data_fetchers import data_fetcher_factory
from dbnd import get_databand_context
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor import pendulum
from dbnd.api.shared_schemas.airflow_monitor import (
    AirflowServerInfo,
    airflow_server_info_schema,
)


logger = logging.getLogger(__name__)


class AirflowInstanceDetails(object):
    def __init__(self, config, since, incomplete_since, server_info, fetcher):
        self.config = config
        self.since = since
        self.incomplete_since = incomplete_since
        self.airflow_server_info = server_info
        self.data_fetcher = fetcher

    def update_airflow_server(
        self,
        airflow_version,
        dags_path,
        logs_path,
        airflow_export_version,
        synced_from,
        active_dags,
    ):
        self.airflow_server_info.airflow_version = airflow_version
        self.airflow_server_info.dags_path = dags_path
        self.airflow_server_info.logs_path = logs_path
        self.airflow_server_info.airflow_export_version = airflow_export_version
        self.airflow_server_info.synced_from = synced_from
        self.airflow_server_info.active_dags = active_dags
        self.airflow_server_info.monitor_error_message = ""

    @property
    def url(self):
        return self.airflow_server_info.base_url


def create_airflow_server_info(airflow_url):
    airflow_server_info = AirflowServerInfo(
        base_url=airflow_url,
        monitor_status="Running",
        airflow_monitor_version=pkg_resources.get_distribution(
            "dbnd_airflow_monitor"
        ).version,
    )

    return airflow_server_info


def get_sync_times_from_api(airflow_server_info):
    """ Update airflow server info with sync times retrieved from API """
    api_client = get_databand_context().databand_api_client
    response = api_client.api_request(
        "airflow_monitor/get_synced_time_frame",
        {},
        "GET",
        query={"base_url": airflow_server_info.base_url},
    )
    fetched_server_info = airflow_server_info_schema.load(response).data
    airflow_server_info.synced_from = fetched_server_info.synced_from
    airflow_server_info.synced_to = fetched_server_info.synced_to
    airflow_server_info.incomplete_synced_to = fetched_server_info.incomplete_synced_to
    airflow_server_info.last_sync_time = fetched_server_info.last_sync_time


def calculate_since_value(
    since_now,
    since,
    sync_history,
    history_only,
    airflow_server_info,
    oldest_incomplete_data_in_days,
    is_incomplete,
):
    default_since = (
        utcnow() - timedelta(days=oldest_incomplete_data_in_days)
        if is_incomplete
        else pendulum.datetime.min
    )

    if since_now:
        final_since_value = utcnow()
    elif since:
        final_since_value = pendulum.parse(since, tz=pytz.UTC)
    elif sync_history or history_only:
        final_since_value = default_since
    else:
        # Default mode
        try:
            get_sync_times_from_api(airflow_server_info)
            final_since_value = (
                airflow_server_info.incomplete_synced_to
                if is_incomplete
                else airflow_server_info.synced_to
            )
            if final_since_value:
                logger.info(
                    "Resuming sync from latest stop at: %s" % (final_since_value,)
                )
            else:
                logger.info(
                    "Latest sync stop not found. Starting sync from the beginning"
                )
                final_since_value = default_since
        except Exception as e:
            logger.info(
                "Could not locate latest sync stop. Exception: {}. Starting Airflow Monitor syncing from the beginning.",
                e,
            )
            final_since_value = default_since

    return final_since_value


def create_airflow_instance_details(monitor_args, configs_fetched, existing_details):
    airflow_instance_details = []
    for fetch_config in configs_fetched:
        for existing_detail in existing_details:
            if existing_detail.config.url == fetch_config.url:
                existing_detail.config = fetch_config
                airflow_instance_details.append(existing_detail)
                break
        else:
            airflow_server_info = create_airflow_server_info(fetch_config.base_url)
            since_value = calculate_since_value(
                monitor_args.since_now,
                monitor_args.since,
                monitor_args.sync_history,
                monitor_args.history_only,
                airflow_server_info,
                fetch_config.oldest_incomplete_data_in_days,
                False,
            )
            incomplete_since_value = calculate_since_value(
                monitor_args.since_now,
                monitor_args.since,
                monitor_args.sync_history,
                monitor_args.history_only,
                airflow_server_info,
                fetch_config.oldest_incomplete_data_in_days,
                True,
            )

            try:
                factory = data_fetcher_factory(fetch_config)
                # We currently use the same value for both since and incomplete_since
                airflow_instance_details.append(
                    AirflowInstanceDetails(
                        fetch_config,
                        since_value,
                        incomplete_since_value,
                        airflow_server_info,
                        factory,
                    )
                )
            except Exception as e:
                logging.error(e)

    return airflow_instance_details


def create_instance_details_list(
    monitor_args, configs_fetched, existing_airflow_instance_details,
):
    if not configs_fetched:
        return []

    airflow_instance_details = create_airflow_instance_details(
        monitor_args, configs_fetched, existing_airflow_instance_details,
    )

    return airflow_instance_details
