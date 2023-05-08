# © Copyright Databand.ai, an IBM Company 2022
import mock.mock
import pytest

from dbnd_dbt_monitor.adapter.dbt_adapter import DbtAdapter
from dbnd_dbt_monitor.data.dbt_config_data import DbtServerConfig
from dbnd_dbt_monitor.multiserver.dbt_services_factory import DbtMonitorServicesFactory

from airflow_monitor.shared.generic_syncer import GenericSyncer
from dbnd._core.utils.uid_utils import get_uuid

from .test_dbt_runs_syncer import (
    MockDbtIntegrationManagementService,
    mock_dbt_integration_management_service,
)


@pytest.fixture
def dbt_integration_management_service_mock() -> MockDbtIntegrationManagementService:
    yield mock_dbt_integration_management_service


@pytest.fixture
def dbt_monitor_services_factory() -> DbtMonitorServicesFactory:
    yield DbtMonitorServicesFactory()


@pytest.fixture
def dbt_server_config() -> DbtMonitorServicesFactory:
    yield DbtServerConfig(
        uid=get_uuid(),
        source_name="test",
        source_type="airflow",
        tracking_source_uid=mock.mock.ANY,
        is_generic_syncer_enabled=True,
    )


class TestDbtServicesFactory:
    def test_get_components(
        self,
        dbt_server_config,
        dbt_integration_management_service_mock,
        dbt_monitor_services_factory,
    ):
        all_components = dbt_monitor_services_factory.get_components(
            integration_config=dbt_server_config,
            integration_management_service=mock_dbt_integration_management_service,
        )
        syncer = all_components[0]
        adapter = syncer.adapter
        assert isinstance(syncer, GenericSyncer)
        assert isinstance(adapter, DbtAdapter)
