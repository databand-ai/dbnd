import attr

from dbnd._core.parameter.parameter_definition import ParameterDefinition


@attr.s(hash=False, repr=False, str=False)
class _ParameterConfig(ParameterDefinition):
    target = attr.ib(default=None)
    key = attr.ib(default=None)
