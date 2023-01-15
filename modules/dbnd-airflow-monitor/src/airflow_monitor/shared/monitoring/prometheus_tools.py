# © Copyright Databand.ai, an IBM Company 2022

import logging

from prometheus_client import Summary


LABEL_NAME_MONITOR = "monitor"
LABEL_NAME_SYNCER = "syncer"
LABEL_NAME_FETCHER = "fetcher"
LABEL_NAMES = [LABEL_NAME_MONITOR, LABEL_NAME_SYNCER, LABEL_NAME_FETCHER]

logger = logging.getLogger(__name__)

query_execution_time = Summary(
    name="dbnd_datasourec_monitor_query_execution_time",
    documentation="Query execution time on a single run for a single component (syncer+fetcher)",
    labelnames=LABEL_NAMES,
)

tracking_report_time = Summary(
    name="dbnd_datasourec_monitor_tracking_report_time",
    documentation="Tracking report time on a single run for a single component (syncer+fetcher)",
    labelnames=LABEL_NAMES,
)
