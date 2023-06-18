# © Copyright Databand.ai, an IBM Company 2022

import logging

from prometheus_client import Counter, Summary


logger = logging.getLogger(__name__)

generic_syncer_assets_data_error_counter = Counter(
    "dbnd_generic_syncer_api_assets_data_fetch_error_counter",
    "Number errors when fetching assets data from api in adapter in generic syncer",
    labelnames=["integration_id", "syncer_instance_id", "error_message"],
)

generic_syncer_error_counter = Counter(
    "dbnd_generic_syncer_error_counter",
    "Number errors in generic syncer",
    labelnames=["integration_id", "syncer_instance_id", "error_message"],
)

generic_syncer_sync_once_total_duration_seconds = Summary(
    "dbnd_generic_syncer_sync_once_total_duration_seconds",
    "The end time minus the start time of sync_once in seconds",
    labelnames=["integration_id", "syncer_instance_id"],
)

generic_syncer_sync_once_batch_duration_seconds = Summary(
    "dbnd_generic_syncer_sync_once_batch_duration_seconds",
    "The end time minus the start time of a batch in sync_once in seconds",
    labelnames=["integration_id", "syncer_instance_id"],
)

generic_syncer_total_failed_assets_requests = Counter(
    "dbnd_generic_syncer_total_failed_assets_requests_counter",
    "The number of failed assets requests (probably due to error) that submitted for retry",
    labelnames=["integration_id", "syncer_instance_id"],
)

generic_syncer_total_max_retry_assets_requests = Counter(
    "dbnd_generic_syncer_total_max_retry_assets_requests_counter",
    "The number of failed assets requests (probably due to error) that reached max retry attempt",
    labelnames=["integration_id", "syncer_instance_id"],
)

generic_syncer_assets_data_batch_size_bytes = Summary(
    "dbnd_generic_syncer_assets_data_batch_size_bytes",
    "The size of assets data batch in bytes",
    labelnames=["integration_id", "syncer_instance_id"],
)

generic_syncer_total_assets_size = Summary(
    "dbnd_generic_syncer_assets_total_assets_size",
    "The total number of assets in sync once iteration",
    labelnames=["integration_id", "syncer_instance_id"],
)

generic_syncer_save_tracking_data_response_time_seconds = Summary(
    "dbnd_generic_syncer_save_tracking_data_response_time_in_seconds",
    "The time in seconds to get response from save_tracking_data",
    labelnames=["integration_id", "syncer_instance_id"],
)

generic_syncer_get_assets_data_response_time_seconds = Summary(
    "dbnd_generic_syncer_get_assets_data_response_time_in_seconds",
    "The time in seconds to get response from get_assets_data",
    labelnames=["integration_id", "syncer_instance_id"],
)


def report_generic_syncer_error(integration_id, syncer_instance_id, error_message):
    generic_syncer_error_counter.labels(
        integration_id=integration_id,
        syncer_instance_id=syncer_instance_id,
        error_message=error_message,
    ).inc()


def report_assets_data_fetch_error(integration_id, syncer_instance_id, error_message):
    generic_syncer_assets_data_error_counter.labels(
        integration_id=integration_id,
        syncer_instance_id=syncer_instance_id,
        error_message=error_message,
    ).inc()


def report_sync_once_total_duration_seconds(
    integration_id, syncer_instance_id, duration
):
    generic_syncer_sync_once_total_duration_seconds.labels(
        integration_id=integration_id, syncer_instance_id=syncer_instance_id
    ).observe(duration)


def report_sync_once_batch_duration_seconds(
    integration_id, syncer_instance_id, duration
):
    generic_syncer_sync_once_batch_duration_seconds.labels(
        integration_id=integration_id, syncer_instance_id=syncer_instance_id
    ).observe(duration)


def report_total_failed_assets_requests(
    integration_id, syncer_instance_id, total_failed_assets
):
    generic_syncer_total_failed_assets_requests.labels(
        integration_id=integration_id, syncer_instance_id=syncer_instance_id
    ).inc(total_failed_assets)


def report_total_assets_max_retry_requests(
    integration_id, syncer_instance_id, total_max_retry_assets
):
    generic_syncer_total_max_retry_assets_requests.labels(
        integration_id=integration_id, syncer_instance_id=syncer_instance_id
    ).inc(total_max_retry_assets)


def report_assets_data_batch_size_bytes(
    integration_id, syncer_instance_id, assets_data_batch_size_bytes
):
    generic_syncer_assets_data_batch_size_bytes.labels(
        integration_id=integration_id, syncer_instance_id=syncer_instance_id
    ).observe(assets_data_batch_size_bytes)


def report_total_assets_size(integration_id, syncer_instance_id, assets_size):
    generic_syncer_total_assets_size.labels(
        integration_id=integration_id, syncer_instance_id=syncer_instance_id
    ).observe(assets_size)


def report_save_tracking_data_response_time(
    integration_id, syncer_instance_id, duration
):
    generic_syncer_save_tracking_data_response_time_seconds.labels(
        integration_id=integration_id, syncer_instance_id=syncer_instance_id
    ).observe(duration)


def report_get_assets_data_response_time(integration_id, syncer_instance_id, duration):
    generic_syncer_get_assets_data_response_time_seconds.labels(
        integration_id=integration_id, syncer_instance_id=syncer_instance_id
    ).observe(duration)
