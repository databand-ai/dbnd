from typing import Dict, Generic, Optional, TypeVar

import attr

from dbnd._core.constants import RESULT_PARAM
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.parameter.parameter_value import ParameterValue


def _ensure_dict(val):
    if isinstance(val, dict):
        return val
    if isinstance(val, list):
        return {p.name: p for p in val}
    if not val:
        return {}
    raise NotImplementedError()


def _merge_dicts(*dicts):
    merged = {}
    for d in dicts:
        merged.update(d)
    return merged


T = TypeVar("T")


@attr.s
class _TaskParams(Generic[T]):
    _class_params = attr.ib(factory=dict, converter=_ensure_dict)
    _user_params = attr.ib(factory=dict, converter=_ensure_dict)
    _user_result_params = attr.ib(factory=dict, converter=_ensure_dict)

    @property
    def result_param(self):
        # type: () -> Optional[T]
        return self._user_result_params.get(RESULT_PARAM)

    @property
    def all_params(self):
        # type: () -> Dict[str, T]
        return _merge_dicts(
            self._class_params, self._user_params, self._user_result_params
        )

    @property
    def class_params(self):
        # type: () -> Dict[str, T]
        return _merge_dicts(self._class_params, self._user_result_params)

    @property
    def user_params(self):
        # type: () -> Dict[str, T]
        return self._user_params

    def get_any_param(self, name, default=None):
        # will get param by name regardless of its type
        for g in (self._user_params, self._user_result_params, self._class_params):
            if name in g:
                return g[name]
        return default

    def clone(self):
        # type: () -> _TaskParams[T]
        return self.__class__(
            class_params=dict(self._class_params),
            user_params=dict(self._user_params),
            user_result_params=dict(self._user_result_params),
        )


TaskDefinitionParams = _TaskParams[ParameterDefinition]
TaskValueParams = _TaskParams[ParameterValue]
