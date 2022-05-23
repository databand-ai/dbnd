from dbnd_dbt_monitor.data.dbt_config_data import DbtServerConfig
from dbnd_dbt_monitor.fetcher.dbt_cloud_data_fetcher import DbtCloudDataFetcher

from airflow_monitor.common.metric_reporter import (
    METRIC_REPORTER,
    decorate_methods,
    measure_time,
)
from dbnd._vendor.tenacity import retry, stop_after_attempt, wait_fixed


def get_data_fetcher(server_config: DbtServerConfig):
    fetcher = DbtCloudDataFetcher.create_from_dbt_credentials(
        dbt_cloud_api_token=server_config.api_token,
        dbt_cloud_account_id=server_config.account_id,
        batch_size=server_config.runs_bulk_size,
        job_id=server_config.job_id,
    )
    return decorate_fetcher(fetcher, server_config.account_id)


def decorate_fetcher(fetcher, label):
    return decorate_methods(
        fetcher,
        DbtCloudDataFetcher,
        measure_time(METRIC_REPORTER.exporter_response_time, label),
        retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(1)),
    )
