# © Copyright Databand.ai, an IBM Company 2022

import logging

from enum import Enum

from prometheus_client import Counter, Summary


LABEL_NAME_INTEGRATION = "integration"
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
