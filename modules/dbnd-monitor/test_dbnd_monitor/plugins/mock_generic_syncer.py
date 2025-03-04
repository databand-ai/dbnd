# © Copyright Databand.ai, an IBM Company 2025
from typing import Optional
from uuid import uuid4

import pytest

from dbnd_monitor.generic_syncer import GenericSyncer
from dbnd_monitor.utils.api_client import ApiClient


@pytest.fixture(scope="function")
def mock_generic_syncer(
    mock_integration_config, mock_tracking_service, mock_reporting_service, mock_adapter
) -> GenericSyncer:  # type: ignore
    yield GenericSyncer(
        config=mock_integration_config,
        tracking_service=mock_tracking_service,
        reporting_service=mock_reporting_service,
        adapter=mock_adapter,
        syncer_instance_id="test_" + str(uuid4()),
    )


class WithGenericSyncer:
    generic_syncer: GenericSyncer

    @property
    def api_client(self) -> Optional[ApiClient]:
        try:
            return self.generic_syncer.reporting_service._api_client
        except AttributeError:
            return None

    @pytest.fixture(autouse=True)
    def setup_generic_syncer(self, mock_generic_syncer):
        self.generic_syncer = mock_generic_syncer
        yield
