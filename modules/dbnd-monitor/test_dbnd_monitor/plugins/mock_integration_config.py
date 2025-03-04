# © Copyright Databand.ai, an IBM Company 2025
from uuid import uuid4

import pytest

from dbnd_monitor.base_integration_config import BaseIntegrationConfig


@pytest.fixture
def mock_integration_config(
    request, mock_monitor_config
) -> BaseIntegrationConfig:  # type: ignore

    config = {
        "uid": uuid4(),
        "integration_type": "airflow",
        "name": "test_integration",
        "credentials": "token",
        "tracking_source_uid": uuid4(),
        "integration_config": {},
    }

    try:
        config.update(getattr(request.cls, "config_extension", {}))
    except (AttributeError, TypeError):
        pass

    yield BaseIntegrationConfig.create(
        config=config, monitor_config=mock_monitor_config
    )
