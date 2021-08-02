import logging

from typing import List, Tuple

from dbnd import config, config_deco, parameter
from dbnd._core.configuration.config_path import ConfigPath
from dbnd.tasks.basics import SimplestTask
from test_dbnd.helpers import value_at_task


logger = logging.getLogger(__name__)


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
