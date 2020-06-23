from dbnd._core.parameter.parameter_builder import (
    PARAMETER_FACTORY,
    ParameterFactory,
    build_parameter,
)
from dbnd._core.parameter.parameters_mapper import ParametersMapper
from targets.values import get_types_registry, get_value_type_of_type


_PARAMS_MAPPER = ParametersMapper()  # type: ParametersMapper


def get_params_mapper():
    return _PARAMS_MAPPER


def register_custom_parameter(value_type, parameter):
    value_type = get_value_type_of_type(value_type, inline_value_type=True)

    _PARAMS_MAPPER.register_custom_parameter(value_type, parameter)
    get_types_registry().register_value_type(value_type)

    return parameter


def get_parameter_for_value_type(value_type):
    return _PARAMS_MAPPER.get_parameter(value_type)
