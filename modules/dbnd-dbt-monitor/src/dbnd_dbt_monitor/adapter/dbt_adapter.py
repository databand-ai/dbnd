# © Copyright Databand.ai, an IBM Company 2022

from typing import Generator, Tuple

from dbnd_dbt_monitor.fetcher.dbt_cloud_data_fetcher import DbtCloudDataFetcher

from airflow_monitor.shared.adapter.adapter import (
    Adapter,
    Assets,
    AssetState,
    AssetToState,
)


class DbtAdapter(Adapter):
    def __init__(self, data_fetcher: DbtCloudDataFetcher):
        self.data_fetcher = data_fetcher

    def init_cursor(self) -> int:
        return self.data_fetcher.get_last_seen_run_id()

    def init_assets_for_cursor(
        self, cursor: object, batch_size: int
    ) -> Generator[Tuple[Assets, str], None, None]:
        # batch_size is configured in data_fetcher from server_config (default value=30)
        new_dbt_run_ids = self.data_fetcher.get_run_ids_to_sync_from_dbt(cursor)
        dbt_assets_to_states = []
        if new_dbt_run_ids:
            cursor = max(new_dbt_run_ids)
            for dbt_run_id in new_dbt_run_ids:
                dbt_assets_to_states.append(
                    AssetToState(asset_id=str(dbt_run_id), state=AssetState.INIT)
                )
        yield (Assets(assets_to_state=dbt_assets_to_states), cursor)

    def get_assets_data(self, assets: Assets, next_page: object) -> Assets:
        pass
