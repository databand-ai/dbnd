from typing import Any

import attr

from dbnd._core.errors import friendly_error
from dbnd._core.errors.friendly_error.task_execution import (
    failed_to_read_value_from_target,
)
from dbnd._core.parameter.parameter_definition import ParameterDefinition, T
from targets.errors import NotSupportedValue
from targets.multi_target import MultiTarget


class ResultProxyTarget(MultiTarget):
    target_no_traverse = True

    def __init__(self, source, names, properties=None):
        super(ResultProxyTarget, self).__init__(
            targets=None, properties=properties, source=source
        )
        self.names = names

    @property
    def targets(self):
        return list(self)

    def get_sub_result(self, name):
        return getattr(self.source_task, name)

    def __iter__(self):
        for name in self.names:
            yield self.get_sub_result(name)

    def __repr__(self):
        return "result(%s)" % ",".join(self.names)

    def __getitem__(self, item):
        if isinstance(item, int):  # handle p[0]
            try:
                item = self.names[item]
            except IndexError:
                raise IndexError(
                    "ResultProxyTarget index out fo the range. This target has only {} elements".format(
                        len(self.names)
                    )
                )
        try:
            return self.get_sub_result(item)
        except AttributeError:
            raise AttributeError("ResultProxyTarget has no `{}` target".format(item))

    def as_dict(self):
        return {name: self.get_sub_result(name) for name in self.names}

    def as_task(self):
        return self.source.task


@attr.s(hash=False, repr=False, str=False)
class FuncResultParameter(ParameterDefinition):
    schema = attr.ib(default=None)

    @property
    def names(self):
        return [p.name for p in self.schema]

    def build_output(self, task):
        return ResultProxyTarget(source=self._target_source(task), names=self.names)

    def dump_to_target(self, target, value, **kwargs):
        """
        We don't need to dump this value, it's just a map to all other outputs
        """

    def _validate_result(self, result):
        if not isinstance(result, (tuple, list, dict)):
            raise friendly_error.task_execution.wrong_return_value_type(
                self.task_definition, self.names, result
            )
        elif len(result) != len(self.schema):
            raise friendly_error.task_execution.wrong_return_value_len(
                self.task_definition, self.names, result
            )
        if isinstance(result, dict):
            if set(result.keys()).symmetric_difference(set(self.names)):
                raise NotSupportedValue(
                    "Returned result doesn't match expected schema. Expected {}, got {}".format(
                        self.names, result.keys()
                    )
                )

    def named_results(self, result):
        self._validate_result(result)
        if isinstance(result, dict):
            return [(name, result[name]) for name in self.names]
        return zip(self.names, result)

    def load_from_target(
        self, target, **kwargs
    ):  # type: (FuncResultParameter, ResultProxyTarget, **Any)-> T
        results = []
        for p in self.schema:
            p_target = target.get_sub_result(p.name)
            try:

                results.append(p.load_from_target(p_target, **kwargs))
            except Exception as ex:
                raise failed_to_read_value_from_target(
                    ex, p_target.source.task, p, target
                )

        return tuple(results)
