import typing

from typing import Any, Dict, Iterable, List, Optional, Tuple

import attr

from dbnd._core.errors import DatabandError
from dbnd._core.parameter.constants import ParameterScope


if typing.TYPE_CHECKING:
    from dbnd._core.parameter.parameter_definition import ParameterDefinition
    from dbnd._core.task.task_with_params import _TaskWithParams


@attr.s(slots=True)
class ParameterValue(object):
    """Holds the runtime value of a parameter and information about its calculation"""

    # reference to the parameter definition
    parameter = attr.ib()  # type: ParameterDefinition
    # the calculated value
    value = attr.ib()  # type: Any

    # name of all the information sources which used to calculate the value
    source = attr.ib()  # type: str
    source_value = attr.ib()  # type: Any

    task = attr.ib(default=None)  # type: _TaskWithParams
    parsed = attr.ib(default=True)  # type: bool

    # any warnings caught while calculating the value
    warnings = attr.ib(factory=list)

    @property
    def name(self):
        return self.parameter.name

    def update_param_value(self, value):
        self.value = value
        if self.task:
            self.task._update_property_on_parameter_value_set(self.name, value)

    def _update_param_value_from_task_set(self, value):
        # handler for task.param=value code
        # we should not call task here, otherwise, we get recursion
        # we assume that value is already set in task body
        self.value = value


def fold_parameter_value(left, right):
    # type: (ParameterValue, Optional[ParameterValue]) -> ParameterValue
    """Merge the values of the two parameters and combine any information needed to identify this merge"""
    if not right or right.value is None:
        return left

    if not left.parameter.value_type.support_merge:
        raise ValueError(
            "value type {} not supporting merge".format(left.parameter.value_type)
        )

    if left.parameter.value_type != right.parameter.value_type:
        raise ValueError(
            "can't merge two value with different types left={} right={}".format(
                left.parameter.value_type, right.parameter.value_type
            )
        )

    new_value = left.parameter.value_type.merge_values(left.value, right.value)

    return ParameterValue(
        parameter=left.parameter,
        value=new_value,
        source=",".join((left.source, right.source)),
        source_value=left.source_value,
        warnings=left.warnings + right.warnings,
    )


@attr.s(slots=True)
class ParameterFilter(object):
    """
    Represent filter settings for Parameters
    """

    significant_only = attr.ib(default=False)
    input_only = attr.ib(default=False)
    output_only = attr.ib(default=False)
    user_only = attr.ib(default=False)

    param_cls = attr.ib(default=None)
    scope = attr.ib(default=None)  # type: Optional[ParameterScope]


class ParameterFilters(object):
    CHILDREN = ParameterFilter(scope=ParameterScope.children)
    USER = ParameterFilter(user_only=True)
    OUTPUTS = ParameterFilter(output_only=True)
    INPUTS = ParameterFilter(input_only=True)
    USER_INPUTS = ParameterFilter(user_only=True, input_only=True)
    USER_OUTPUTS = ParameterFilter(user_only=True, output_only=True)
    SIGNIFICANT_ONLY = ParameterFilter(significant_only=True)
    SIGNIFICANT_INPUTS = ParameterFilter(input_only=True, significant_only=True)
    NONE = ParameterFilter()


class Parameters(object):
    def __init__(self, source, param_values):
        # type: (str,  List[ParameterValue]) -> None

        self.source = source
        self._param_values = param_values
        self._param_values_map = {p.parameter.name: p for p in self._param_values}

    def get_param_value_safe(self, param_name):  # type: (str) -> ParameterValue
        pv = self.get_param_value(param_name)
        if not pv:
            raise DatabandError(
                "Param value '%s' is not found at %s" % (param_name, self.source)
            )
        return pv

    def update_param_value(self, param_name, value):
        self.get_param_value_safe(param_name).update_param_value(value)

    def get_param_value(self, param_name):  # type: (str) -> Optional[ParameterValue]
        return self._param_values_map.get(param_name, None)

    def get_param(self, param_name):  # type: (str) -> Optional[ParameterDefinition]
        p = self.get_param_value(param_name)
        if p:
            return p.parameter
        return None

    def get_value(self, param_name):
        p = self.get_param_value(param_name)
        if not p:
            raise DatabandError(
                "Parameter '%s' not found at %s" % (param_name, self.source)
            )
        return p.value

    def _filter_params(self, param_filter):
        # type: (ParameterFilter)-> Iterable[ParameterValue]
        result = self._param_values  # type: Iterable[ParameterValue]

        pf = param_filter or ParameterFilters.NONE
        if pf.param_cls is not None:
            result = filter(lambda p: isinstance(p.parameter, pf.param_cls), result)
        if pf.significant_only:
            result = filter(lambda p: p.parameter.significant, result)
        if pf.input_only:
            result = filter(lambda p: not p.parameter.is_output(), result)
        if pf.output_only:
            result = filter(lambda p: p.parameter.is_output(), result)
        if pf.user_only:
            result = filter(lambda p: not p.parameter.system, result)
        if pf.scope:
            result = filter(lambda p: p.parameter.scope == pf.scope, result)
        return result

    def get_params(self, param_filter=None):
        # type: (Optional[ParameterFilter])-> List[ParameterDefinition]
        return [pv.parameter for pv in self._filter_params(param_filter=param_filter)]

    def get_param_values(self, param_filter=None):
        # type: (Optional[ParameterFilter])-> List[ParameterValue]
        return list(self._filter_params(param_filter=param_filter))

    def get_params_with_value(self, param_filter=None):
        # type: (Optional[ParameterFilter])-> List[ Tuple[ParameterDefinition, Any]]
        return [
            (pv.parameter, pv.value)
            for pv in self._filter_params(param_filter=param_filter)
        ]

    # TODO: change name to "to_string"
    def get_params_serialized(self, param_filter=None):
        # type: (Optional[ParameterFilter])-> List[ Tuple[ParameterDefinition, Any]]
        return [
            (pv.parameter.name, pv.parameter.signature(pv.value))
            for pv in self._filter_params(param_filter=param_filter)
        ]

    def get_params_signatures(self, param_filter=None):
        # type: (Optional[ParameterFilter])-> List[ Tuple[ParameterDefinition, Any]]
        return [
            (pv.parameter.name, pv.parameter.signature(pv.value))
            for pv in self._filter_params(param_filter=param_filter)
        ]

    def to_env_map(self, task, *param_names):
        # type: (_TaskWithParams, List[str]) -> Dict[str, str]
        if param_names:
            params = [self.get_param_value(param_name) for param_name in param_names]
        else:
            params = self.get_param_values()
        return {
            p.parameter.get_env_key(task.task_name): p.parameter.to_str(p.value)
            for p in params
        }

    def get_param_env_key(self, task, param_name):
        return self.get_param(param_name).get_env_key(task.task_name)

    def __iter__(self):
        # type: (Parameters) -> Iterable[ParameterValue]
        """helper function to iterate all the params with it's definition, value and meta"""
        for pv in self._param_values:
            yield pv

    def as_key_value_dict(self):
        return {p.name: p.value for p in self.get_param_values()}
