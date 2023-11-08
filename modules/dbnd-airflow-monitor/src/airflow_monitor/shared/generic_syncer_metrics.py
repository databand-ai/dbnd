# © Copyright Databand.ai, an IBM Company 2022

import logging

from prometheus_client import Gauge, Summary


logger = logging.getLogger(__name__)

generic_syncer_total_failed_assets_requests = Gauge(
    "dbnd_generic_syncer_total_failed_assets_requests",
    "The number of failed assets requests (probably due to error) that submitted for retry",
    labelnames=["integration_id", "syncer_instance_id"],
)

generic_syncer_total_max_retry_assets_requests = Gauge(
    "dbnd_generic_syncer_total_max_retry_assets_requests",
    "The number of failed assets requests (probably due to error) that reached max retry attempt",
    labelnames=["integration_id", "syncer_instance_id"],
)

generic_syncer_total_assets_size = Gauge(
    "dbnd_generic_syncer_assets_total_assets_size",
    "The total number of assets in sync once iteration",
    labelnames=["integration_id", "syncer_instance_id"],
)


func_execution_time = Summary(
    "dbnd_generic_syncer_func_execution_time",
    "Function execution time",
    labelnames=("integration_id", "syncer_instance_id", "func_name"),
)


def report_total_failed_assets_requests(
    integration_id, syncer_instance_id, total_failed_assets
):
    generic_syncer_total_failed_assets_requests.labels(
        integration_id=integration_id, syncer_instance_id=syncer_instance_id
    ).set(total_failed_assets)


def report_total_assets_max_retry_requests(
    integration_id, syncer_instance_id, total_max_retry_assets
):
    generic_syncer_total_max_retry_assets_requests.labels(
        integration_id=integration_id, syncer_instance_id=syncer_instance_id
    ).set(total_max_retry_assets)


def report_total_assets_size(integration_id, syncer_instance_id, assets_size):
    generic_syncer_total_assets_size.labels(
        integration_id=integration_id, syncer_instance_id=syncer_instance_id
    ).set(assets_size)
