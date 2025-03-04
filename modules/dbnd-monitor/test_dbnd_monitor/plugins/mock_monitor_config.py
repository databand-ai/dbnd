# © Copyright Databand.ai, an IBM Company 2025
import pytest

from dbnd_monitor.base_monitor_config import BaseMonitorConfig


@pytest.fixture
def mock_monitor_config(request, mock_environment) -> BaseMonitorConfig:  # type: ignore
    overrides = {}

    try:
        overrides.update(getattr(request.cls, "overrides", {}))
    except (AttributeError, TypeError):
        pass

    yield BaseMonitorConfig.from_env(**overrides)
