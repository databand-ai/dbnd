# © Copyright Databand.ai, an IBM Company 2022
from typing import Dict, List
from urllib.parse import parse_qsl, urlparse

from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.datastage_client.datastage_assets_client import (
    DataStageAssetsClient,
)
from dbnd_datastage_monitor.syncer.datastage_runs_syncer import format_datetime

from airflow_monitor.shared.adapter.adapter import Adapter, AdapterData
from dbnd._core.utils.date_utils import parse_datetime
from dbnd._core.utils.timezone import utcnow


class DataStageAdapter(Adapter):
    def __init__(
        self,
        config: DataStageServerConfig,
        datastage_assets_client: DataStageAssetsClient,
    ):
        super(DataStageAdapter, self).__init__(config)
        self.datastage_asset_client = datastage_assets_client
        self.last_cursor = None

    def get_last_cursor(self) -> str:
        if not self.last_cursor:
            return format_datetime(utcnow())
        return self.last_cursor

    def get_new_data(self, cursor: str, batch_size: int, next_page: str) -> AdapterData:
        full_runs = None
        failed_runs = None
        start_date = parse_datetime(cursor)
        end_date = format_datetime(utcnow())
        self.datastage_asset_client.client.page_size = batch_size
        new_runs, next_page = self.datastage_asset_client.get_new_runs(
            start_time=format_datetime(start_date),
            end_time=end_date,
            next_page=next_page,
        )
        if new_runs:
            full_runs, failed_runs = self.datastage_asset_client.get_full_runs(
                new_runs.values()
            )
        if next_page == None:
            self.last_cursor = end_date
        return AdapterData(data=full_runs, failed=failed_runs, next_page=next_page)

    def get_update_data(self, to_update: List[str]) -> Dict[str, object]:
        project_runs_to_update = []
        for run_link in to_update:
            project_id = self._extract_project_id_from_url(run_link)
            if project_id == self.datastage_asset_client.client.project_id:
                project_runs_to_update.append(run_link)
        # do not submit failed runs on update, already reported to server as in progress
        (full_runs, failed_runs) = self.datastage_asset_client.get_full_runs(
            project_runs_to_update
        )
        return full_runs

    def _extract_project_id_from_url(self, url: str):
        parsed_url = urlparse(url)
        parsed_query_string = dict(parse_qsl(parsed_url.query))
        project_id = parsed_query_string.get("project_id")
        return project_id
