# © Copyright Databand.ai, an IBM Company 2022

from typing import Generator, Tuple

from dbnd_dbt_monitor.fetcher.dbt_cloud_data_fetcher import DbtCloudDataFetcher

from airflow_monitor.shared.adapter.adapter import Adapter, Assets


class DbtAdapter(Adapter):
    def __init__(self, data_fetcher: DbtCloudDataFetcher):
        self.data_fetcher = data_fetcher

    def init_cursor(self) -> int:
        return self.data_fetcher.get_last_seen_run_id()

    def init_assets_for_cursor(
        self, cursor: object, batch_size: int
    ) -> Generator[Tuple[Assets, str], None, None]:
        pass

    def get_assets_data(self, assets: Assets, next_page: object) -> Assets:
        pass
