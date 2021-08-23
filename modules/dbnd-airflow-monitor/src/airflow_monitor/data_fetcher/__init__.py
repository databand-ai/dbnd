from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.common.metric_reporter import (
    METRIC_REPORTER,
    decorate_methods,
    measure_time,
)
from airflow_monitor.data_fetcher.base_data_fetcher import AirflowDataFetcher
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from airflow_monitor.data_fetcher.file_data_fetcher import FileFetcher
from airflow_monitor.data_fetcher.google_compose_data_fetcher import (
    GoogleComposerFetcher,
)
from airflow_monitor.data_fetcher.web_data_fetcher import WebFetcher
from dbnd._core.errors import DatabandConfigError
from tenacity import retry, stop_after_attempt, wait_fixed


FETCHERS = {
    "db": DbFetcher,
    "web": WebFetcher,
    "composer": GoogleComposerFetcher,
    "file": FileFetcher,
}


def get_data_fetcher(server_config: AirflowServerConfig) -> AirflowDataFetcher:
    fetcher = FETCHERS[server_config.fetcher]
    if fetcher:
        return decorate_fetcher(fetcher(server_config), server_config.base_url)

    err = "Unsupported fetcher_type: {}, use one of the following: {}".format(
        server_config.fetcher, "/".join(FETCHERS.keys())
    )
    raise DatabandConfigError(err, help="Please specify correct fetcher type")


def decorate_fetcher(fetcher, label):
    return decorate_methods(
        fetcher,
        AirflowDataFetcher,
        measure_time(METRIC_REPORTER.exporter_response_time, label),
        retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(1)),
    )
