from airflow_monitor.common.config_data import AirflowServerConfig
from airflow_monitor.common.metric_reporter import (
    METRIC_REPORTER,
    decorate_measure_time,
)
from airflow_monitor.data_fetcher.base_data_fetcher import AirflowDataFetcher
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from airflow_monitor.data_fetcher.file_data_fetcher import FileFetcher
from airflow_monitor.data_fetcher.google_compose_data_fetcher import (
    GoogleComposerFetcher,
)
from airflow_monitor.data_fetcher.web_data_fetcher import WebFetcher
from dbnd._core.errors import DatabandConfigError


FETCHERS = {
    "db": DbFetcher,
    "web": WebFetcher,
    "composer": GoogleComposerFetcher,
    "file": FileFetcher,
}


def get_data_fetcher(server_config: AirflowServerConfig):
    fetcher = FETCHERS[server_config.fetcher]
    if fetcher:
        return decorate_fetcher(fetcher(server_config), server_config.base_url)

    err = "Unsupported fetcher_type: {}, use one of the following: {}".format(
        server_config.fetcher, "/".join(FETCHERS.keys())
    )
    raise DatabandConfigError(err, help="Please specify correct fetcher type")


def decorate_fetcher(fetcher, label):
    return decorate_measure_time(
        fetcher, AirflowDataFetcher, METRIC_REPORTER.exporter_response_time, label,
    )
