# © Copyright Databand.ai, an IBM Company 2025
import pytest

from dbnd_monitor.multiserver import MultiServerMonitor


@pytest.fixture
def mock_multiserver_monitor(
    request, mock_monitor_config, mock_integration_management_service
) -> MultiServerMonitor:  # type: ignore
    integration_types = {}

    try:
        integration_types.update(getattr(request.cls, "integration_types", []))
    except (AttributeError, TypeError):
        pass

    yield MultiServerMonitor(
        monitor_config=mock_monitor_config,
        integration_management_service=mock_integration_management_service,
        integration_types=integration_types,
    )
