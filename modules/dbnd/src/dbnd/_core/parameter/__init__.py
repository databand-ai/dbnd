# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.parameter.parameter_builder import (  # noqa: 401
    PARAMETER_FACTORY,
    ParameterFactory,
    build_parameter,
)
from dbnd._core.parameter.parameter_definition import infer_parameter_value_type
from dbnd._core.parameter.parameter_value import ParameterValue
from dbnd._core.parameter.parameters_mapper import ParametersMapper
from dbnd._core.utils.basics.nothing import NOTHING
from targets.values import get_value_type_of_obj, register_value_type
from targets.values.builtins_values import DefaultObjectValueType


_PARAMS_MAPPER = ParametersMapper()  # type: ParametersMapper


def get_params_mapper():
    return _PARAMS_MAPPER


def register_custom_parameter(value_type, parameter):
    value_type = register_value_type(value_type)
    _PARAMS_MAPPER.register_custom_parameter(value_type, parameter)
    return parameter


def get_parameter_for_value_type(value_type):
    return _PARAMS_MAPPER.get_parameter(value_type)


def build_user_parameter_value(name, value, source):
    """
    Build parameter value for user defined name and value
    """
    value_type = get_value_type_of_obj(value, default_value_type=DefaultObjectValueType)
    param_f = get_parameter_for_value_type(value_type)
    param = build_parameter(param_f)
    param.name = name

    if value is NOTHING:
        parameter, warnings = param, []
        actual_value = param.default
    else:
        parameter, warnings = infer_parameter_value_type(param, value)
        actual_value = value

    return ParameterValue(
        parameter=parameter,
        source=source,
        source_value=value,
        value=actual_value,
        parsed=False,
        warnings=warnings,
    )
