import pytest

from dbnd import Config, config, parameter, task
from dbnd._core.errors import DatabandBuildError, TaskClassNotFoundException
from dbnd._core.task_build.task_registry import get_task_registry


class TConfig(Config):
    c_value = parameter[str]


@task(t_config=parameter[TConfig])
def t_with_config(t_config):
    # type: (TConfig)-> str
    return t_config.c_value


class TestParametersAsConfigObjects(object):
    def test_can_build(self):
        with config({"t_config": {"_type": "TConfig", "c_value": "value"}}):
            expected = t_with_config.task("t_config")
            expected.dbnd_run()

    def test_can_not_find(self):
        with pytest.raises(TaskClassNotFoundException):
            get_task_registry().get_task_cls("t_config")

    def test_can_not_build_not_found(self):
        with pytest.raises(DatabandBuildError):
            expected = t_with_config.task("t_config")
            expected.dbnd_run()

    def test_can_not_build_no_value(self):
        with pytest.raises(DatabandBuildError):
            with config({"t_config": {"_type": "TConfig"}}):
                expected = t_with_config.task("t_config")
                expected.dbnd_run()
