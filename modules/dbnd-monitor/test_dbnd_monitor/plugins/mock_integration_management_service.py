# © Copyright Databand.ai, an IBM Company 2025
from unittest.mock import patch

import pytest

from dbnd_monitor.integration_management_service import IntegrationManagementService


@pytest.fixture
def mock_integration_management_service():
    with patch("dbnd.utils.api_client.ApiClient.api_request"):
        yield IntegrationManagementService()


@pytest.mark.usefixtures("mock_integration_management_service")
class WithIntegrationManagementService:
    integration_management_service: IntegrationManagementService

    @property
    def api_request(self):
        return self.integration_management_service._api_client.api_request

    @pytest.fixture(autouse=True)
    def setup(self, mock_integration_management_service):
        self.integration_management_service = mock_integration_management_service
        yield
