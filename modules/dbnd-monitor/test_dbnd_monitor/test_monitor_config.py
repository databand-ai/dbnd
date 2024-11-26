# © Copyright Databand.ai, an IBM Company 2022

import os

from random import choice
from typing import Optional

import attr
import pytest

from mock import patch

from dbnd_monitor.base_monitor_config import BaseMonitorConfig


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
        (
            {
                "SOME_ENV_PREFIX__COMPONENT_ERROR_SUPPORT": choice(
                    ["y", "yes", "t", "true", "1"]
                )
            },
            {},
            {"component_error_support": True},
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
