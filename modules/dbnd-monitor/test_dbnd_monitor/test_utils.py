# © Copyright Databand.ai, an IBM Company 2024

import os

import pytest

from mock import patch

from dbnd_monitor.utils.api_client import TrackingServiceConfig


@pytest.fixture
def env_config() -> dict:
    return {
        "DBND__CORE__DATABAND_URL": "fake_databand_url",
        "DBND__CORE__DATABAND_ACCESS_TOKEN": "fake_token",
    }


@pytest.mark.parametrize(
    "additional_env_config, should_ignore_ssl_errors",
    [
        ({}, False),
        ({"DBND__CORE__IGNORE_SSL_ERRORS": "True"}, True),
        ({"DBND__CORE__IGNORE_SSL_ERRORS": "False"}, False),
    ],
)
def test_tracking_source_config_ignore_ssl_error_flag(
    env_config: dict, additional_env_config: dict, should_ignore_ssl_errors: bool
):
    env_config.update(additional_env_config)

    with patch.dict(os.environ, env_config):
        tracking_source_config = TrackingServiceConfig.from_env()
        assert tracking_source_config.ignore_ssl_errors == should_ignore_ssl_errors
