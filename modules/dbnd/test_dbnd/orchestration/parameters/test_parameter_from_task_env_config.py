# © Copyright Databand.ai, an IBM Company 2022

import logging

import pytest

from dbnd import Config, parameter
from dbnd._core.errors import DatabandBuildError
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase
from dbnd_test_scenarios.test_common.task.factories import TTask


logger = logging.getLogger(__name__)


class TestParameterFromTaskEnvConfig(TargetTestBase):
    def test_simple_task_with_param_from_env_config(self):
        class TTaskWithParamFromEnvConfig(TTask):
            spark_config = parameter(from_task_env_config=True)[str]
            docker_engine = parameter(from_task_env_config=True)[str]

        t_task = TTaskWithParamFromEnvConfig(docker_engine="custom_engine")

        assert t_task.spark_config == "spark"
        assert t_task.docker_engine == "custom_engine"

    def test_no_env_in_config(self):
        class TConfigWithParamFromEnvConfig(Config):
            spark_config = parameter(from_task_env_config=True)[str]
            docker_engine = parameter(from_task_env_config=True)[str]

        with pytest.raises(DatabandBuildError):
            TConfigWithParamFromEnvConfig(docker_engine="custom_engine")

    def test_unknown_param_from_env(self):
        class TWithParamFromEnvConfig(TTask):
            unknown = parameter(from_task_env_config=True)[str]
            docker_engine = parameter(from_task_env_config=True)[str]

        with pytest.raises(DatabandBuildError):
            TWithParamFromEnvConfig(docker_engine="custom_engine")
