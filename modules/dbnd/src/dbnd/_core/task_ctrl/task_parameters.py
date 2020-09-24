from typing import Any, Dict, List, Tuple, Type

from dbnd._core.configuration.config_value import ConfigValue
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.utils.basics.text_banner import safe_string


class TaskParameters(object):
    def __init__(self, task):
        self.task = task
        self.task_meta = self.task.task_meta
        self._params = [p_value.parameter for p_value in self.task_meta.task_params]

        self._param_obj_map = {p.name: p for p in self._params}
        self._param_meta_map = {
            p_value.name: p_value for p_value in self.task_meta.task_params
        }

    def get_param(self, param_name):
        return self._param_obj_map.get(param_name, None)

    def get_value(self, param_name):
        # we dont' want to autoread when we run this function
        # most cases we are running it from "banner" print
        # so if we are in the autoread we will use original values
        task_auto_read_original = self.task._task_auto_read_original
        if task_auto_read_original is not None:
            return task_auto_read_original[param_name]

        return getattr(self.task, param_name)

    def get_param_meta(self, param_name):  # type: (str)->ConfigValue
        return self._param_meta_map.get(param_name, None)

    def get_params(
        self,
        param_cls=ParameterDefinition,
        significant_only=False,
        input_only=False,
        output_only=False,
        user_only=False,
    ):
        # type: (...)-> List[ParameterDefinition]
        result = self._params
        if param_cls is not None:
            result = filter(lambda p: isinstance(p, param_cls), result)
        if significant_only:
            result = filter(lambda p: p.significant, result)
        if input_only:
            result = filter(lambda p: not p.is_output(), result)
        if output_only:
            result = filter(lambda p: p.is_output(), result)
        if user_only:
            result = filter(lambda p: not p.system, result)
        return list(result)

    def get_param_values(
        self,
        param_cls=ParameterDefinition,
        significant_only=False,
        input_only=False,
        output_only=False,
        user_only=False,
    ):
        # type: (Type[ParameterDefinition], bool, bool, bool, bool) -> List[ Tuple[ParameterDefinition, Any]]
        result = self.get_params(
            param_cls=param_cls,
            significant_only=significant_only,
            input_only=input_only,
            output_only=output_only,
            user_only=user_only,
        )

        result = [(p, self.get_value(p.name)) for p in result]
        return result

    # TODO: change name to "to_string"
    def get_params_serialized(
        self, param_cls=ParameterDefinition, significant_only=False, input_only=False
    ):
        return [
            (p.name, p.signature(value))
            for p, value in self.get_param_values(
                param_cls=param_cls,
                significant_only=significant_only,
                input_only=input_only,
            )
        ]

    def get_param_value_origin(self, param_name):
        # type: (str) -> str
        # Returns where param was created, e.g. ctor/CLI argument/env var/config
        from dbnd._core.task_ctrl.task_visualiser import _MAX_VALUE_SIZE

        param_meta = self.get_param_meta(param_name)
        value_source = param_meta.source if param_meta else ""
        value_origin = safe_string(value_source, _MAX_VALUE_SIZE)
        return value_origin

    def to_env_map(self, *param_names):
        # type: (List[str]) -> Dict[str, str]
        return {
            p.get_env_key(self.task.task_name): p.to_str(value)
            for p, value in self.get_param_values()
            if value is not None and (len(param_names) == 0 or p.name in param_names)
        }

    def get_param_env_key(self, param_name):
        return self.get_param(param_name).get_env_key(self.task.task_name)
