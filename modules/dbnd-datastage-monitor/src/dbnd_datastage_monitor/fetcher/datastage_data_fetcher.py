# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import Dict

from dbnd_datastage_monitor.datastage_client.datastage_assets_client import (
    DataStageAssetsClient,
)


logger = logging.getLogger(__name__)


class DataStageDataFetcher:
    def __init__(self, datastage_runs_getter, project_id):
        self.datastage_runs_getter: DataStageAssetsClient = datastage_runs_getter
        self._project_id = project_id

    def get_runs_to_sync(self, start_date: str, end_date: str) -> Dict[str, str]:
        next_page = None
        all_runs = {}
        while True:
            new_runs, next_page = self.datastage_runs_getter.get_new_runs(
                start_time=start_date, end_time=end_date, next_page=next_page
            )
            if new_runs:
                all_runs.update(new_runs)

            if next_page is None:
                break

        return all_runs

    def get_full_runs(self, runs):
        datastage_full_runs = self.datastage_runs_getter.get_full_runs(runs_links=runs)
        return datastage_full_runs
