# © Copyright Databand.ai, an IBM Company 2022

import logging

from collections import defaultdict
from typing import Dict, List

from dbnd_datastage_monitor.datastage_client.datastage_assets_client import (
    DataStageAssetsClient,
)


logger = logging.getLogger(__name__)


class MultiProjectDataStageDataFetcher:
    def __init__(self, datastage_project_clients: List[DataStageAssetsClient]):
        self.project_asset_clients = datastage_project_clients
        self._access_map = {
            client.project_id: client for client in datastage_project_clients
        }

    def get_runs_to_sync(
        self, start_date: str, end_date: str
    ) -> Dict[str, Dict[str, str]]:
        next_page = None
        all_runs = defaultdict(dict)
        for datastage_runs_getter in self.project_asset_clients:
            logger.info(
                "Checking for new runs for project %s", datastage_runs_getter.project_id
            )
            while True:
                new_runs, next_page = datastage_runs_getter.get_new_runs(
                    start_time=start_date, end_time=end_date, next_page=next_page
                )
                if new_runs:
                    project_runs = all_runs[datastage_runs_getter.project_id]
                    project_runs.update(new_runs)

                if next_page is None:
                    break

        return all_runs

    def get_full_runs(self, runs, project_id: str):
        logger.info("Retrieving full runs for project %s", project_id)
        runs_getter: DataStageAssetsClient = self._access_map.get(project_id)
        if not runs_getter:
            logger.warning(
                "Received run of project id %s which is not in the projects configured for the syncer",
                project_id,
            )
            return None

        return runs_getter.get_full_runs(runs_links=runs)
