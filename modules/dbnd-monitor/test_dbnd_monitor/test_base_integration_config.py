# © Copyright Databand.ai, an IBM Company 2024

import os
import uuid

from unittest.mock import patch

import pytest

from dbnd_monitor.base_integration_config import BaseIntegrationConfig
from dbnd_monitor.base_monitor_config import BaseMonitorConfig


@pytest.mark.parametrize(
    "value,expected",
    [
        ("yes", True),
        ("y", True),
        ("true", True),
        ("t", True),
        ("1", True),
        ("YES", True),
        ("Y", True),
        ("TRUE", True),
        ("T", True),
        ("N/A", False),
        ("0", False),
        ("n", False),
        ("no", False),
        ("f", False),
        ("false", False),
        ("N", False),
        ("NO", False),
        ("F", False),
        ("FALSE", False),
    ],
)
def test_base_integration_config_create(value, expected):
    """
    The test creates a BaseIntegrationConfig object and checks
    the `component_error_support` attribute value.
    """

    c = {
        "uid": uuid.UUID("12345678123456781234567812345678"),
        "integration_config": {},
        "integration_type": "test",
        "name": "test",
        "credentials": "test",
    }

    with patch.dict(
        os.environ, {"DBND__MONITOR__COMPONENT_ERROR_SUPPORT": value}
    ) as patched:
        if value == "N/A":
            del patched["DBND__MONITOR__COMPONENT_ERROR_SUPPORT"]
        monitor_config = BaseMonitorConfig.from_env()

    base_integration_config = BaseIntegrationConfig.create(c, monitor_config)
    assert base_integration_config.component_error_support is expected


def test_base_integration_config_create_no_monitor_config():
    """
    The test creates a BaseIntegrationConfig object and checks
    the `component_error_support` attribute value.
    """

    c = {
        "uid": uuid.UUID("12345678123456781234567812345678"),
        "integration_config": {},
        "integration_type": "test",
        "name": "test",
        "credentials": "test",
    }

    base_integration_config = BaseIntegrationConfig.create(c)
    assert base_integration_config.component_error_support is False
