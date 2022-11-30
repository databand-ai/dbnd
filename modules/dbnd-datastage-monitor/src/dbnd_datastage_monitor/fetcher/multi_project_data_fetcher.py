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
        self.project_asset_clients = {
            client.project_id: client for client in datastage_project_clients
        }
        self.datastage_projects_ids = set(self.project_asset_clients.keys())

    def get_runs_to_sync(
        self, start_date: str, end_date: str
    ) -> Dict[str, Dict[str, str]]:
        next_page = None
        all_runs = defaultdict(dict)
        for datastage_runs_getter in self.project_asset_clients.values():
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
        runs_getter: DataStageAssetsClient = self.project_asset_clients.get(project_id)
        if not runs_getter:
            logger.warning(
                "Received run of project id %s which is not in the projects configured for the syncer",
                project_id,
            )
            return None

        return runs_getter.get_full_runs(runs_links=runs)

    def _is_projects_update_needed(self, updated_project_ids: List[str]) -> bool:
        difference = set(updated_project_ids) ^ set(self.project_asset_clients.keys())
        return len(difference) > 0

    def update_projects(self, datastage_project_clients: List[DataStageAssetsClient]):
        if not self._is_projects_update_needed(
            [client.project_id for client in datastage_project_clients]
        ):
            return
        logger.info("Project changes detected, updating fetcher projects")
        updated_clients = {}
        for asset_client in datastage_project_clients:
            asset_client_to_add = asset_client
            if asset_client.project_id in self.project_asset_clients:
                # use the existing client instead of the new one
                asset_client_to_add = self.project_asset_clients.get(
                    asset_client.project_id
                )

            updated_clients[asset_client_to_add.project_id] = asset_client_to_add

        self.project_asset_clients = updated_clients
