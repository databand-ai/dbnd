# © Copyright Databand.ai, an IBM Company 2022

import logging

import pytest

from dbnd import Config, config, parameter, task
from dbnd._core.errors import DatabandBuildError
from dbnd_run.tasks.basics import SimplestTask


logger = logging.getLogger(__name__)


class _TestConfig(Config):
    c_value = parameter[str]


class TTaskwithConfigParam(SimplestTask):
    p_config = parameter[_TestConfig]


@task
def ttask_with_config_param(p_config=parameter[_TestConfig]):
    return p_config.c_value


class TestParameterConfigObj(object):
    def test_value_config(self):
        conf = {_TestConfig.c_value: "tvalue"}
        with config(conf):
            actual = TTaskwithConfigParam(p_config="_TestConfig")
            assert actual.p_config.c_value == "tvalue"

    def test_value_not_defined(self):
        with pytest.raises(DatabandBuildError):
            actual = TTaskwithConfigParam(p_config="_TestConfig")
            assert actual.p_config.c_value == "tvalue"

    def test_decorator_config(self):
        conf = {_TestConfig.c_value: "tvalue"}
        with config(conf):
            actual = ttask_with_config_param.task(_TestConfig())
            assert actual.p_config.c_value == "tvalue"
