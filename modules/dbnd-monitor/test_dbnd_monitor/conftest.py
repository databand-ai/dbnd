# © Copyright Databand.ai, an IBM Company 2022

from unittest.mock import patch
from uuid import uuid4

import pytest

from dbnd_monitor.base_integration_config import BaseIntegrationConfig
from dbnd_monitor.generic_syncer import GenericSyncer


INTEGRATION_UID = uuid4()


pytest_plugins = ["test_dbnd_monitor.plugins"]


@pytest.fixture
def mock_config() -> BaseIntegrationConfig:  # type: ignore
    yield BaseIntegrationConfig(
        uid=INTEGRATION_UID,
        source_name="test_syncer",
        source_type="integration",
        tracking_source_uid="12345",
        sync_interval=10,
    )


@pytest.fixture
def generic_runtime_syncer(
    mock_tracking_service, mock_config, mock_reporting_service, mock_adapter
):
    syncer = GenericSyncer(
        config=mock_config,
        tracking_service=mock_tracking_service,
        reporting_service=mock_reporting_service,
        adapter=mock_adapter,
        syncer_instance_id="123",
    )
    with (
        patch.object(syncer, "refresh_config", new=lambda *args: None),
        patch.object(syncer, "tracking_service", wraps=syncer.tracking_service),
        patch.object(syncer, "reporting_service", wraps=syncer.reporting_service),
    ):
        yield syncer
