import typing

from typing import Any

import attr


if typing.TYPE_CHECKING:
    from dbnd._core.parameter.parameter_definition import ParameterDefinition


@attr.s(slots=True)
class ParameterValue(object):
    parameter = attr.ib()  # type: ParameterDefinition
    value = attr.ib()  # type: Any

    source = attr.ib()  # type: str
    source_value = attr.ib()  # type: Any

    parsed = attr.ib(True)  # type: bool
    warnings = attr.ib(factory=list)

    @property
    def name(self):
        return self.parameter.name
