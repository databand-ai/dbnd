# © Copyright Databand.ai, an IBM Company 2022

import attr

from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.utils.task_utils import to_targets


@attr.s(hash=False, repr=False, str=False)
class _TargetParameter(ParameterDefinition):  # extracted for generics support
    """
    Parameter whose value is a ```target```.

    In the task definition, use

    .. code-block:: python

        class MyTask(dbnd.Task):
            some_input = data

            def run(self):
                  self.some_input.open('r')

    """

    def normalize(self, x):
        # can not move to value_type, we need target_config
        return to_targets(x, from_string_kwargs=dict(config=self.target_config))
