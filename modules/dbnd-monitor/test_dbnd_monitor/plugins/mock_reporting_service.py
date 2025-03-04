# © Copyright Databand.ai, an IBM Company 2025
from unittest.mock import patch

import pytest

from dbnd_monitor.reporting_service import ReportingService


@pytest.fixture
def mock_reporting_service(request) -> ReportingService:  # type: ignore
    aggregator = None

    try:
        aggregator = getattr(request.cls, "aggregator", None)
    except AttributeError:
        aggregator = None

    with patch("dbnd.utils.api_client.ApiClient.api_request"):
        if aggregator is None:
            reporting_service = ReportingService("airflow")
        else:
            reporting_service = ReportingService("airflow", aggregator)
        yield reporting_service
