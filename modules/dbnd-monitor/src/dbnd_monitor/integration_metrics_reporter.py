# © Copyright Databand.ai, an IBM Company 2022

import logging

from prometheus_client import Counter, Summary


logger = logging.getLogger(__name__)

DEFAULT_LABELS = ("integration_id", "tracking_source_uid", "syncer_type")


integration_assets_in_state = Counter(
    "dbnd_integration_assets_in_state",
    "Assets count per state at the end of iteration",
    labelnames=DEFAULT_LABELS + ("asset_state",),
)

integration_total_assets_size = Counter(
    "dbnd_integration_assets_total_assets_size",
    "The total number of assets in sync once iteration",
    labelnames=DEFAULT_LABELS + ("asset_source",),
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

    def report_assets_in_state(self, asset_state: str, count: int) -> None:
        integration_assets_in_state.labels(
            **self.default_labels, asset_state=asset_state
        ).inc(count)

    def execution_time(self, func_name: str) -> Summary:
        """
        Returns Summary object to track time. Example usage:

        with metrics_reporter.execution_time("process_data").time():
            ... process data
        """
        return func_execution_time.labels(**self.default_labels, func_name=func_name)

    def report_total_assets_size(self, asset_source: str, asset_count: int) -> None:
        integration_total_assets_size.labels(
            **self.default_labels, asset_source=asset_source
        ).inc(asset_count)
