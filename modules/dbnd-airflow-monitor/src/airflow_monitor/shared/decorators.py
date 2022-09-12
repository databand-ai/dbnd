# © Copyright Databand.ai, an IBM Company 2022

from airflow_monitor.common.metric_reporter import (
    METRIC_REPORTER,
    decorate_methods,
    measure_time,
)
from dbnd._vendor.tenacity import retry, stop_after_attempt, wait_fixed


def decorate_tracking_service(tracking_service, label):
    return decorate_methods(
        tracking_service,
        type(tracking_service),
        measure_time(METRIC_REPORTER.dbnd_api_response_time, label),
        retry(stop=stop_after_attempt(2), reraise=True),
    )


def decorate_configuration_service(configuration_service):
    return decorate_methods(
        configuration_service,
        type(configuration_service),
        measure_time(METRIC_REPORTER.dbnd_api_response_time, "global"),
        retry(stop=stop_after_attempt(2), reraise=True),
    )


def decorate_fetcher(fetcher, label):
    return decorate_methods(
        fetcher,
        type(fetcher),
        measure_time(METRIC_REPORTER.exporter_response_time, label),
        retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(1)),
    )
