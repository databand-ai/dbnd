import logging
import typing

import six

from dbnd._core.constants import TaskEssence
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.parameter.parameter_value import ParameterFilters
from dbnd._core.task_ctrl.task_ctrl import TaskSubCtrl
from dbnd._core.utils.basics.nothing import is_defined
from dbnd._core.utils.basics.text_banner import safe_string


if typing.TYPE_CHECKING:
    pass
logger = logging.getLogger(__name__)

_MAX_PARAM_VALUE_AT_DB = 3000


def _safe_params(params):
    return [
        (name, safe_string(value, _MAX_PARAM_VALUE_AT_DB)) for name, value in params
    ]


def _parameter_value_to_argparse_str(p, p_value):
    formatted_value = p.as_str_input(p_value)
    if isinstance(p_value, list) or isinstance(p_value, dict):
        formatted_value = '"{}"'.format(formatted_value)
    if p.name == "task_env":
        return ["--env", "{}".format(formatted_value)]
    return ["--set", "{}={}".format(p.name, formatted_value)]


class TaskRepr(TaskSubCtrl):
    def __init__(self, task):
        super(TaskRepr, self).__init__(task)
        self.task_command_line = None
        self.task_functional_call = None

    def initialize(self):
        self.task_command_line = self.calculate_command_line_for_task()
        self.task_functional_call = self.calculate_task_call()

    def __get_relevant_params(self):
        relevant = []
        for p, value in self.params.get_params_with_value(ParameterFilters.USER_INPUTS):
            if is_defined(p.default):
                try:
                    same_value = value == p.default
                except Exception:
                    same_value = p.signature(value) == p.signature(p.default)
                if same_value:
                    continue
            relevant.append((p, value))
        return sorted(relevant, key=lambda x: x[0].name)

    def __get_override_repr(self):
        if not TaskEssence.ORCHESTRATION.is_instance(self.task):
            # config and overrides are exists in orchestration mode only
            return {}
        if not self.task.task_config_override:
            return {}
        result = {}
        for k, v in six.iteritems(self.task.task_config_override):
            if isinstance(k, ParameterDefinition):
                k = "%s.%s" % (k.task_definition.task_family, k.name)
            result[k] = v
        return result

    def calculate_command_line_for_task(self):

        task_name = self.task.task_family
        base = "dbnd run {task_name} ".format(task_name=task_name)

        params = []
        for p, p_value in self.__get_relevant_params():
            params.extend(_parameter_value_to_argparse_str(p, p_value))

        overrides = self.__get_override_repr()
        if overrides:
            for k, v in sorted(overrides.items()):
                overrides_str = "'{param_name}':{param_value}".format(
                    param_name=k, param_value=repr(v)
                )
                params.append('--conf "{ %s }"' % overrides_str)

        if params:
            base += " ".join(params)
        return base

    def task_repr(self):
        params = []
        for p, p_value in self.__get_relevant_params():
            param = "{param_name}={param_value}".format(
                param_name=p.name, param_value=p.to_repr(p_value)
            )
            params.append(param)

        overrides = self.__get_override_repr()
        if overrides:
            for k, v in sorted(overrides.items()):
                overrides_str = "'{param_name}':{param_value}".format(
                    param_name=k, param_value=repr(v)
                )
                params.append("overrides={ %s }" % overrides_str)

        task_ref = "{task_name}".format(
            task_name=self.task.task_definition.full_task_family
        )
        if self.task.task_decorator:
            task_ref += ".task"
        return "{task_ref}({params})".format(
            task_ref=task_ref, params=", ".join(params)
        )

    def calculate_task_call(self):
        task_repr = self.task_repr()
        return "{task_repr}.dbnd_run()".format(task_repr=task_repr)
