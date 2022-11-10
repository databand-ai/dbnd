# © Copyright Databand.ai, an IBM Company 2022

import logging

from prometheus_client import Counter, Summary


logger = logging.getLogger(__name__)


datastage_fetch_error_counter = Counter(
    "dbnd_datastage_monitor_error_counter",
    "Number errors in fetching data from DataStage",
    labelnames=["function_name", "error_message"],
)

datastage_api_error_counter = Counter(
    "dbnd_datastage_monitor_datastage_api_error_counter",
    "Number errors in fetching data from DataStage",
    labelnames=["status_code", "url"],
)

datastage_runs_list_duration = Summary(
    "dbnd_datastage_monitor_runs_list_window_duration_in_seconds",
    "The end time minus the start time of the list runs window in seconds",
    labelnames=["tracking_source_uid"],
)

datastage_runs_collection_delay = Summary(
    "dbnd_datastage_monitor_runs_collection_delay_in_seconds",
    "The delay from the first run start time to the time it was collected by the monitor in seconds",
    labelnames=["tracking_source_uid", "project_uid"],
)

datastage_found_runs_not_inited = Counter(
    "dbnd_datastage_monitor_run_init_failures_sum",
    "The number of runs found in DataStage but not initialized (probably due to error)",
    labelnames=["tracking_source_uid", "project_uid"],
)


def report_error(function_name, error_message):
    datastage_fetch_error_counter.labels(
        function_name=function_name, error_message=error_message
    ).inc()


def report_api_error(status_code, url):
    datastage_api_error_counter.labels(status_code=status_code, url=url).inc()


def report_list_duration(tracking_source_uid, duration):
    datastage_runs_list_duration.labels(tracking_source_uid).observe(duration)


def report_runs_collection_delay(tracking_source_uid, project_uid, delay):
    datastage_runs_collection_delay.labels(
        project_uid=project_uid, tracking_source_uid=tracking_source_uid
    ).observe(delay)


def report_runs_not_initiated(tracking_source_uid, project_uid, number_of_runs):
    datastage_found_runs_not_inited.labels(
        tracking_source_uid=tracking_source_uid, project_uid=project_uid
    ).inc(number_of_runs)
