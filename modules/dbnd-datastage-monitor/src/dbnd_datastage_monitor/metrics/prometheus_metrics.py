# © Copyright Databand.ai, an IBM Company 2022

import logging

from prometheus_client import Counter, Gauge, Summary


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

datastage_run_request_retry_delay = Summary(
    "dbnd_datastage_monitor_run_request_retry_delay_in_seconds",
    "The delay from the run start time to the time it was collected after retries by the monitor in seconds",
    labelnames=["tracking_source_uid", "run_uid"],
)

datastage_found_runs_not_inited = Counter(
    "dbnd_datastage_monitor_run_init_failures_counter",
    "The number of runs found in DataStage but not initialized (probably due to error)",
    labelnames=["tracking_source_uid", "project_uid"],
)

datastage_submitted_run_request_retries_to_error_queue = Counter(
    "dbnd_datastage_monitor_run_request_retries_submitted_to_error_queue_counter",
    "The number of failed get run requests (probably due to error) that submitted to error queue for retry",
    labelnames=["tracking_source_uid", "project_uid"],
)

datastage_run_request_retries_fetched_from_error_queue_to_retry = Counter(
    "dbnd_datastage_monitor_run_request_retries_fetched_from_error_queue_to_retry_counter",
    "The number of failed get run requests (probably due to error) that fetched from error queue for retry",
    labelnames=["tracking_source_uid", "project_uid"],
)


datastage_run_request_retries_queue_size = Gauge(
    "dbnd_datastage_monitor_run_request_retries_queue_size_gauge",
    "The error queue size of get run requests to retry",
    labelnames=["tracking_source_uid"],
)

datastage_run_request_retries_cache_size = Gauge(
    "dbnd_datastage_monitor_run_request_retries_cache_size_gauge",
    "The cache size of failed get run requests to retry",
    labelnames=["tracking_source_uid"],
)

datastage_completely_failed_run_request_retry = Counter(
    "dbnd_datastage_monitor_run_request_retries_fetched_that_exceeded_max_retries_counter",
    "The number of failed run retry requests (probably due to error) that exceeded max retries",
    labelnames=["tracking_source_uid", "project_uid"],
)


datastage_in_progress_failed_run_request_retries_skipped = Counter(
    "dbnd_datastage_monitor_update_run_requests_retries_skipped",
    "The number of runs monitor tried to update but failed to fetch",
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


def report_run_request_retry_delay(tracking_source_uid, run_uid, delay):
    datastage_run_request_retry_delay.labels(
        run_uid=run_uid, tracking_source_uid=tracking_source_uid
    ).observe(delay)


def report_runs_not_initiated(tracking_source_uid, project_uid, number_of_runs):
    datastage_found_runs_not_inited.labels(
        tracking_source_uid=tracking_source_uid, project_uid=project_uid
    ).inc(number_of_runs)


def report_run_request_retry_submitted_to_error_queue(
    tracking_source_uid, project_uid, number_of_runs
):
    datastage_submitted_run_request_retries_to_error_queue.labels(
        tracking_source_uid=tracking_source_uid, project_uid=project_uid
    ).inc(number_of_runs)


def report_run_request_retry_fetched_from_error_queue(tracking_source_uid, project_uid):
    datastage_run_request_retries_fetched_from_error_queue_to_retry.labels(
        tracking_source_uid=tracking_source_uid, project_uid=project_uid
    ).inc()


def report_run_request_retry_queue_size(tracking_source_uid, queue_size):
    datastage_run_request_retries_cache_size.labels(
        tracking_source_uid=tracking_source_uid
    ).set(queue_size)


def report_run_request_retry_cache_size(tracking_source_uid, cache_size):
    datastage_run_request_retries_queue_size.labels(
        tracking_source_uid=tracking_source_uid
    ).set(cache_size)


def report_completely_run_request_retry_failed(tracking_source_uid, project_uid):
    datastage_completely_failed_run_request_retry.labels(
        tracking_source_uid=tracking_source_uid, project_uid=project_uid
    ).inc()


def report_in_progress_failed_run_request_skipped(
    tracking_source_uid, project_uid, number_of_runs
):
    datastage_in_progress_failed_run_request_retries_skipped.labels(
        tracking_source_uid=tracking_source_uid, project_uid=project_uid
    ).inc(number_of_runs)
