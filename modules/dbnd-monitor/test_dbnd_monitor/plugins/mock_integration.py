# © Copyright Databand.ai, an IBM Company 2025
from typing import List

import pytest

from dbnd_monitor.base_component import BaseComponent
from dbnd_monitor.base_integration import BaseIntegration
from dbnd_monitor.base_integration_config import BaseIntegrationConfig


class MockIntegrationConfig(BaseIntegrationConfig):
    pass


class MockIntegration(BaseIntegration):

    MONITOR_TYPE = "airflow"
    CONFIG_CLASS = BaseIntegrationConfig

    config: BaseIntegrationConfig

    def __init__(self, integration_config, reporting_service=None, components=None):
        super().__init__(integration_config, reporting_service)
        self._components = components or []

    def get_components(self) -> List[BaseComponent]:
        return self._components


@pytest.fixture
def mock_integration():
    yield MockIntegration(
        integration_config=None, reporting_service=None, components=None
    )
