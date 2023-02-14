# © Copyright Databand.ai, an IBM Company 2022

from unittest.mock import MagicMock

import pytest

from dbnd_datastage_monitor.data.datastage_config_data import DataStageServerConfig
from dbnd_datastage_monitor.datastage_client.datastage_assets_client import (
    ConcurrentRunsGetter,
)
from dbnd_datastage_monitor.multiserver.datastage_services_factory import (
    DataStageMonitorServicesFactory,
)
from dbnd_datastage_monitor.syncer.datastage_runs_syncer import DataStageRunsSyncer

from airflow_monitor.shared.generic_syncer import GenericSyncer


@pytest.fixture
def mock_server_config_generic_syncer_disabled() -> DataStageServerConfig:
    yield DataStageServerConfig(
        source_name="test_syncer",
        source_type="integration",
        tracking_source_uid="12345",
        sync_interval=10,
        number_of_fetching_threads=2,
        project_ids=["1", "2"],
    )


@pytest.fixture
def mock_server_config_generic_syncer_enabled() -> DataStageServerConfig:
    yield DataStageServerConfig(
        source_name="test_syncer",
        source_type="integration",
        tracking_source_uid="12345",
        sync_interval=10,
        number_of_fetching_threads=2,
        project_ids=["1", "2"],
        is_generic_syncer_enabled=True,
    )


@pytest.fixture
def monitor_services_factory() -> DataStageMonitorServicesFactory:
    yield DataStageMonitorServicesFactory()


@pytest.fixture
def mock_syncer_management_service() -> MagicMock:
    return MagicMock()


class TestDataStageMonitorServicesFactory:
    def test_get_components_generic_syncer_disabled(
        self,
        mock_server_config_generic_syncer_disabled,
        monitor_services_factory,
        mock_syncer_management_service,
    ):
        all_components = monitor_services_factory.get_components(
            server_config=mock_server_config_generic_syncer_disabled,
            syncer_management_service=mock_syncer_management_service,
        )
        assert len(all_components) == 1
        result_syncer = all_components[0]
        assert isinstance(result_syncer, DataStageRunsSyncer)
        assert result_syncer.SYNCER_TYPE == "datastage_runs_syncer"
        result_data_fetcher = result_syncer.data_fetcher
        assert len(result_data_fetcher.project_asset_clients) == 2
        isinstance(
            result_data_fetcher.project_asset_clients.get("1"), ConcurrentRunsGetter
        )
        isinstance(
            result_data_fetcher.project_asset_clients.get("2"), ConcurrentRunsGetter
        )

    def test_get_components_generic_syncer_enabled(
        self,
        mock_server_config_generic_syncer_enabled,
        monitor_services_factory,
        mock_syncer_management_service,
    ):
        all_components = monitor_services_factory.get_components(
            server_config=mock_server_config_generic_syncer_enabled,
            syncer_management_service=mock_syncer_management_service,
        )
        assert len(all_components) == 2
        expected_project_id = 1
        for result_syncer in all_components:
            assert isinstance(result_syncer, GenericSyncer)
            assert result_syncer.SYNCER_TYPE == "generic_syncer"
            result_adapter = result_syncer.adapter
            assert result_adapter.datastage_asset_client.project_id == str(
                expected_project_id
            )
            expected_project_id += 1
