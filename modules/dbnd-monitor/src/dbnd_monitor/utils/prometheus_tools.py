# © Copyright Databand.ai, an IBM Company 2022

import logging

from enum import Enum

from prometheus_client import Counter, Gauge, Summary


LABEL_NAME_INTEGRATION = "integration"
LABEL_NAME_MONITOR = "monitor"
LABEL_NAME_SYNCER = "syncer"
LABEL_NAME_FETCHER = "fetcher"
LABEL_NAME_STATUS = "status"


class MonitorSyncStatus(Enum):
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"


logger = logging.getLogger(__name__)

fetching_execution_time = Summary(
    name="dbnd_datasource_monitor_fetching_execution_time",
    documentation="Query execution time on a single run for a single component (syncer+fetcher)",
    labelnames=[LABEL_NAME_INTEGRATION, LABEL_NAME_SYNCER, LABEL_NAME_FETCHER],
)

tracking_report_time = Summary(
    name="dbnd_datasource_monitor_tracking_report_time",
    documentation="Tracking report time on a single run for a single component (syncer+fetcher)",
    labelnames=[LABEL_NAME_INTEGRATION, LABEL_NAME_SYNCER, LABEL_NAME_FETCHER],
)

monitor_sync_iteration = Counter(
    name="dbnd_monitor_sync_iteration",
    documentation="Reports a sync completion with relevant status",
    labelnames=[
        LABEL_NAME_INTEGRATION,
        LABEL_NAME_SYNCER,
        LABEL_NAME_FETCHER,
        LABEL_NAME_STATUS,
    ],
)

sync_once_time = Summary(
    name="dbnd_monitor_sync_once_time",
    documentation="Complete sync time on a single run for a single component (syncer+fetcher)",
    labelnames=[LABEL_NAME_INTEGRATION, LABEL_NAME_SYNCER, LABEL_NAME_FETCHER],
)

monitor_iteration_time = Summary(
    name="dbnd_monitor_iteration_time",
    documentation="Complete sync time of full iteration (all components)",
)

monitor_integrations_count = Gauge(
    name="dbnd_monitor_integrations_count",
    documentation="Current number of active integrations",
)

integration_iteration_time = Summary(
    name="dbnd_monitor_integration_iteration_time",
    documentation="Sync time of single integration (all components)",
    labelnames=[LABEL_NAME_INTEGRATION, LABEL_NAME_MONITOR, LABEL_NAME_FETCHER],
)

integration_components_count = Gauge(
    name="dbnd_monitor_components_count",
    documentation="Current number of components per integration",
    labelnames=[LABEL_NAME_INTEGRATION, LABEL_NAME_MONITOR, LABEL_NAME_FETCHER],
)
