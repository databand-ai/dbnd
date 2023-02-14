# © Copyright Databand.ai, an IBM Company 2022
from datetime import timedelta
from typing import Dict, List

from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.datastage_client.datastage_assets_client import (
    DataStageAssetsClient,
)
from dbnd_datastage_monitor.syncer.datastage_runs_syncer import format_datetime

from airflow_monitor.shared.adapter.adapter import Adapter
from dbnd._core.utils.date_utils import parse_datetime
from dbnd._core.utils.timezone import utcnow


class DataStageAdapter(Adapter):
    def __init__(
        self,
        config: DataStageServerConfig,
        datastage_assets_client: DataStageAssetsClient,
    ):
        self.config = config
        self.datastage_asset_client = datastage_assets_client
        self.last_cursor = None

    def get_last_cursor(self) -> str:
        if not self.last_cursor:
            return format_datetime(utcnow())
        return self.last_cursor

    def get_data(
        self, cursor: str, batch_size: int, next_page: str
    ) -> (Dict[str, object], List[str], str):
        full_runs = None
        failed_runs = None
        interval = timedelta(self.config.fetching_interval_in_minutes)
        start_date = parse_datetime(cursor)
        end_date = start_date + interval
        self.datastage_asset_client.client.page_size = batch_size
        new_runs, next_page = self.datastage_asset_client.get_new_runs(
            start_time=format_datetime(start_date),
            end_time=format_datetime(end_date),
            next_page=next_page,
        )
        if new_runs:
            full_runs, failed_runs = self.datastage_asset_client.get_full_runs(
                new_runs.values()
            )
        if next_page == None:
            self.last_cursor = end_date
        return full_runs, failed_runs, next_page
