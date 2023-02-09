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
from dbnd._core.utils.uid_utils import get_uuid


@pytest.fixture
def mock_server_config_generic_syncer_disabled() -> DataStageServerConfig:
    yield DataStageServerConfig(
        uid=get_uuid(),
        source_name="test_syncer",
        source_type="integration",
        tracking_source_uid=get_uuid(),
        sync_interval=10,
        number_of_fetching_threads=2,
        project_ids=["1", "2"],
        is_generic_syncer_enabled=False,
    )


@pytest.fixture
def mock_server_config_generic_syncer_enabled() -> DataStageServerConfig:
    yield DataStageServerConfig(
        uid=get_uuid(),
        source_name="test_syncer",
        source_type="integration",
        tracking_source_uid=get_uuid(),
        sync_interval=10,
        number_of_fetching_threads=2,
        project_ids=["1", "2"],
        is_generic_syncer_enabled=True,
    )


@pytest.fixture
def monitor_services_factory() -> DataStageMonitorServicesFactory:
    yield DataStageMonitorServicesFactory()


@pytest.fixture
def mock_integration_management_service() -> MagicMock:
    return MagicMock()


class TestDataStageMonitorServicesFactory:
    def test_get_components_generic_syncer_disabled(
        self,
        mock_server_config_generic_syncer_disabled,
        monitor_services_factory,
        mock_integration_management_service,
    ):
        all_components = monitor_services_factory.get_components(
            integration_config=mock_server_config_generic_syncer_disabled,
            integration_management_service=mock_integration_management_service,
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
        mock_integration_management_service,
    ):
        all_components = monitor_services_factory.get_components(
            integration_config=mock_server_config_generic_syncer_enabled,
            integration_management_service=mock_integration_management_service,
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
