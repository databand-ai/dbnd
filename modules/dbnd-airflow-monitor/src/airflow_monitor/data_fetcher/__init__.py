from airflow_monitor.common import AirflowServerConfig
from airflow_monitor.data_fetcher.base_data_fetcher import AirflowDataFetcher
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from airflow_monitor.data_fetcher.file_data_fetcher import FileFetcher
from airflow_monitor.data_fetcher.google_compose_data_fetcher import (
    GoogleComposerFetcher,
)
from airflow_monitor.data_fetcher.web_data_fetcher import WebFetcher


FETCHERS = {
    "db": DbFetcher,
    "web": WebFetcher,
    "composer": GoogleComposerFetcher,
    "file": FileFetcher,
}


def get_data_fetcher(server_config: AirflowServerConfig):
    fetcher = FETCHERS[server_config.fetcher]
    if fetcher:
        return fetcher(server_config)

    err = "Unsupported fetcher_type: {}, use one of the following: {}".format(
        server_config.fetcher, "/".join(FETCHERS.keys())
    )
    raise Exception(err)
