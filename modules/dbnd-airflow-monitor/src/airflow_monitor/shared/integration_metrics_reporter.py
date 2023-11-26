# © Copyright Databand.ai, an IBM Company 2022

import logging

from prometheus_client import Gauge, Summary


logger = logging.getLogger(__name__)

DEFAULT_LABELS = ("integration_id", "tracking_source_uid", "syncer_type")

integration_total_failed_assets_requests = Gauge(
    "dbnd_integration_total_failed_assets_requests",
    "The number of failed assets requests (probably due to error) that submitted for retry",
    labelnames=DEFAULT_LABELS,
)

integration_total_max_retry_assets_requests = Gauge(
    "dbnd_integration_total_max_retry_assets_requests",
    "The number of failed assets requests (probably due to error) that reached max retry attempt",
    labelnames=DEFAULT_LABELS,
)

integration_total_assets_size = Gauge(
    "dbnd_integration_assets_total_assets_size",
    "The total number of assets in sync once iteration",
    labelnames=DEFAULT_LABELS,
)

func_execution_time = Summary(
    "dbnd_integration_func_execution_time",
    "Function execution time",
    labelnames=DEFAULT_LABELS + ("func_name",),
)


class IntegrationMetricsReporter:
    def __init__(self, integration_id: str, tracking_source_uid: str, syncer_type: str):
        self.default_labels = {
            "integration_id": integration_id,
            "tracking_source_uid": tracking_source_uid,
            "syncer_type": syncer_type,
        }

    def report_total_failed_assets_requests(self, total_failed_assets):
        integration_total_failed_assets_requests.labels(**self.default_labels).set(
            total_failed_assets
        )

    def report_total_assets_max_retry_requests(self, total_max_retry_assets):
        integration_total_max_retry_assets_requests.labels(**self.default_labels).set(
            total_max_retry_assets
        )

    def execution_time(self, func_name: str) -> Summary:
        """
        Returns Summary object to track time. Example usage:

        with metrics_reporter.execution_time("process_data").time():
            ... process data
        """
        return func_execution_time.labels(**self.default_labels, func_name=func_name)

    def report_total_assets_size(self, assets_size):
        integration_total_assets_size.labels(**self.default_labels).set(assets_size)
