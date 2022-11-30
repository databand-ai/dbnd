# © Copyright Databand.ai, an IBM Company 2022
from typing import List
from unittest.mock import Mock

from dbnd_datastage_monitor.fetcher.multi_project_data_fetcher import (
    MultiProjectDataStageDataFetcher,
)


class TestMultiProjectDataFetcher:
    project_id_1 = "a48124f870b711edb8487e26100d6e04"
    project_id_2 = "a3d8c20470b711edb8487e26100d6e04"

    def get_datastage_mocked_assets(self, project_ids: List[str]) -> List[Mock]:
        return [Mock(project_id=proj_id) for proj_id in project_ids]

    def test_update_projects_replacing_existing_project(self):
        # Arrange
        datastage_clients_init_args = self.get_datastage_mocked_assets(
            [self.project_id_1]
        )

        multi_proj_data_fetcher = MultiProjectDataStageDataFetcher(
            datastage_clients_init_args
        )

        # Act
        update_project_args = self.get_datastage_mocked_assets([self.project_id_2])
        multi_proj_data_fetcher.update_projects(update_project_args)

        # Assert
        assert len(multi_proj_data_fetcher.project_asset_clients) == 1
        assert self.project_id_2 in multi_proj_data_fetcher.project_asset_clients
        assert self.project_id_1 not in multi_proj_data_fetcher.project_asset_clients

        assert (
            multi_proj_data_fetcher.project_asset_clients.get(self.project_id_2)
            == update_project_args[0]
        )

    def test_update_projects_only_adding_projects(self):
        # Arrange
        datastage_clients_init_args = self.get_datastage_mocked_assets(
            [self.project_id_1]
        )

        multi_proj_data_fetcher = MultiProjectDataStageDataFetcher(
            datastage_clients_init_args
        )

        # Act
        update_project_args = self.get_datastage_mocked_assets(
            [self.project_id_1, self.project_id_2]
        )
        multi_proj_data_fetcher.update_projects(update_project_args)

        # Assert
        assert len(multi_proj_data_fetcher.project_asset_clients) == 2
        assert self.project_id_2 in multi_proj_data_fetcher.project_asset_clients
        assert self.project_id_1 in multi_proj_data_fetcher.project_asset_clients

        # Make sure we use the previous instance and not the new one
        assert (
            multi_proj_data_fetcher.project_asset_clients.get(self.project_id_1)
            == datastage_clients_init_args[0]
        )
        assert (
            multi_proj_data_fetcher.project_asset_clients.get(self.project_id_1)
            != update_project_args[0]
        )
        assert (
            multi_proj_data_fetcher.project_asset_clients.get(self.project_id_2)
            == update_project_args[1]
        )

    def test_update_projects_with_no_new_projects(self):
        # Arrange
        datastage_clients_init_args = self.get_datastage_mocked_assets(
            [self.project_id_1, self.project_id_2]
        )

        multi_proj_data_fetcher = MultiProjectDataStageDataFetcher(
            datastage_clients_init_args
        )

        # Act
        update_project_args = self.get_datastage_mocked_assets(
            [self.project_id_1, self.project_id_2]
        )
        multi_proj_data_fetcher.update_projects(update_project_args)

        # Assert
        assert len(multi_proj_data_fetcher.project_asset_clients) == 2
        assert self.project_id_2 in multi_proj_data_fetcher.project_asset_clients
        assert self.project_id_1 in multi_proj_data_fetcher.project_asset_clients

        # Make sure we use the previous instance and not the new one
        assert (
            multi_proj_data_fetcher.project_asset_clients.get(self.project_id_1)
            == datastage_clients_init_args[0]
        )
        assert (
            multi_proj_data_fetcher.project_asset_clients.get(self.project_id_2)
            == datastage_clients_init_args[1]
        )
