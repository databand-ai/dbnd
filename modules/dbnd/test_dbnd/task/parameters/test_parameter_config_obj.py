import logging

import pytest

from dbnd import Config, config, parameter
from dbnd._core.errors import DatabandBuildError
from dbnd.tasks.basics import SimplestTask


logger = logging.getLogger(__name__)


class TestConfig(Config):
    c_value = parameter[str]


class TTaskwithConfigParam(SimplestTask):
    p_config = parameter[TestConfig]


class TestParameterConfigObj(object):
    def test_value_config(self):
        conf = {TestConfig.c_value: "tvalue"}
        with config(conf):
            actual = TTaskwithConfigParam(p_config="TestConfig")
            assert actual.p_config.c_value == "tvalue"

    def test_value_not_defined(self):
        with pytest.raises(DatabandBuildError):
            actual = TTaskwithConfigParam(p_config="TestConfig")
            assert actual.p_config.c_value == "tvalue"
