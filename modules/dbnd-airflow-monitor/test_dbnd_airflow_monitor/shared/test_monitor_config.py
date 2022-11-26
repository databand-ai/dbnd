# © Copyright Databand.ai, an IBM Company 2022

import os

from typing import Optional

import attr
import pytest

from mock import patch

from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.shared.base_monitor_config import BaseMonitorConfig


@pytest.mark.parametrize(
    "env, overrides, result",
    [
        ({}, {}, {"custom_number": 123, "custom_string": None}),
        ({"SOME_ENV_PREFIX__CUSTOM_NUMBER": "42"}, {}, {"custom_number": 42}),
        ({}, {"custom_number": 9000}, {"custom_number": 9000}),
        (
            {"SOME_ENV_PREFIX__CUSTOM_NUMBER": "4200"},
            {"custom_number": 9001},
            {"custom_number": 9001},
        ),
        (
            {
                "SOME_ENV_PREFIX__PROMETHEUS_PORT": "123456",
                "SOME_ENV_PREFIX__CUSTOM_STRING": "hello",
                "SOME_ENV_PREFIX__NUMBER_OF_ITERATIONS": 3,
            },
            {"custom_number": 888, "stop_after": None},
            {
                "prometheus_port": 123456,
                "custom_number": 888,
                "custom_string": "hello",
                "stop_after": None,
                "number_of_iterations": 3,
            },
        ),
    ],
)
def test_monitor_config_from_env(env, overrides, result):
    @attr.s(auto_attribs=True)
    class InheritedConfig(BaseMonitorConfig):
        _env_prefix = "SOME_ENV_PREFIX__"

        custom_number: int = attr.ib(default=123, converter=int)
        custom_string: Optional[str] = None

    defaults = attr.asdict(InheritedConfig())
    # make sure env values are strings
    env = {k: str(v) for k, v in env.items()}
    with patch.dict(os.environ, env):
        # check that loading from env and explicit overriding works in correct order
        config = InheritedConfig.from_env(**overrides)
        assert attr.asdict(config) == {**defaults, **result}


def test_airflow_monitor_config_from_env():
    env = {
        "DBND__AIRFLOW_MONITOR__SYNCER_NAME": "amazing_syncer",
        "DBND__AIRFLOW_MONITOR__IS_SYNC_ENABLED": "true",
    }
    with patch.dict(os.environ, env):
        airflow_config = AirflowMonitorConfig.from_env()
        assert airflow_config.syncer_name == "amazing_syncer"
        assert airflow_config.is_sync_enabled is True

    env = {
        "DBND__AIRFLOW_MONITOR__SYNCER_NAME": "awful_syncer",
        "DBND__AIRFLOW_MONITOR__IS_SYNC_ENABLED": "False",
    }
    with patch.dict(os.environ, env):
        airflow_config = AirflowMonitorConfig.from_env()
        assert airflow_config.syncer_name == "awful_syncer"
        assert airflow_config.is_sync_enabled is False
