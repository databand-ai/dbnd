# © Copyright Databand.ai, an IBM Company 2022

import logging

from typing import List, Tuple

from dbnd import config, config_deco, parameter
from dbnd._core.configuration.config_path import ConfigPath
from dbnd_run.tasks.basics import SimplestTask
from dbnd_run.testing.helpers import TTask


logger = logging.getLogger(__name__)


def value_at_task(parameter):
    """
    A hackish way to get the "value" of a parameter.
    """

    class _DummyTask(TTask):
        param = parameter

    return _DummyTask().param


class TestParameterConfig(object):
    def test_full_name_config(self):
        conf = {
            SimplestTask.task_definition.full_task_family: {
                "simplest_param": "from_config"
            }
        }
        with config(conf):
            assert SimplestTask().simplest_param == "from_config"

    def test_parameter_config(self):
        conf = {SimplestTask.simplest_param: "from_config"}
        with config(conf):
            assert SimplestTask().simplest_param == "from_config"

    def test_list_config(self):
        with config({"foo": {"bar": "[1,2,3]"}}):
            assert value_at_task(
                parameter[List[str]](config_path=ConfigPath(section="foo", key="bar"))
            )

    @config_deco({"foo": {"bar": "((1,2),(3,4))"}})
    def test_tuple_config(self):
        assert value_at_task(parameter.config(section="foo", name="bar")[Tuple])

    @config_deco({"foo": {"bar": "3"}})
    def test_choice_parameter(self):
        p = parameter.config(section="foo", name="bar").choices([1, 2, 3])[int]
        assert 3 == value_at_task(p)
