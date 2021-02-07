import logging
import os
import typing

from itertools import chain
from typing import Any, Dict, List, Type

import six

from dbnd._core.constants import RESULT_PARAM, TaskEssence, _TaskParamContainer
from dbnd._core.current import (
    get_databand_context,
    get_databand_run,
    try_get_current_task,
)
from dbnd._core.decorator.schemed_result import FuncResultParameter
from dbnd._core.decorator.task_decorator_spec import args_to_kwargs
from dbnd._core.parameter import build_user_parameter_value
from dbnd._core.parameter.parameter_value import (
    ParameterFilters,
    Parameters,
    ParameterValue,
)
from dbnd._core.task.base_task import _BaseTask
from dbnd._core.task.task_mixin import _TaskCtrlMixin
from dbnd._core.task_build.task_definition import TaskDefinition
from dbnd._core.task_build.task_passport import TaskPassport
from dbnd._core.task_build.task_source_code import TaskSourceCode
from dbnd._core.task_ctrl.task_ctrl import TrackingTaskCtrl
from dbnd._core.task_ctrl.task_output_builder import windows_drive_re
from dbnd._core.task_ctrl.task_relations import traverse_and_set_target
from dbnd._core.utils.basics.memoized import cached
from dbnd._core.utils.basics.nothing import NOTHING
from dbnd._core.utils.timezone import utcnow
from targets import target
from targets.target_config import folder
from targets.utils.path import no_trailing_slash


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun

logger = logging.getLogger(__name__)


class TrackingTask(_BaseTask, _TaskCtrlMixin, _TaskParamContainer):
    """
    Task for tracking only use-cases.

    this task should not contain any class body Parameters definition
    """

    task_essence = TaskEssence.TRACKING
    #############################
    # BACKWARD COMPATIBLE TO `Task`
    task_class_version = ""
    task_is_dynamic = False
    task_is_system = False

    @classmethod
    def for_func(cls, task_definition, task_args, task_kwargs, task_name=None):
        # type: (Type[TrackingTask], TaskDefinition, List[Any], Dict[str,Any], str) -> TrackingTask
        param_values = build_func_parameter_values(
            task_definition, task_args, task_kwargs
        )

        # we need to add RESULT param
        if RESULT_PARAM in task_definition.task_param_defs:
            parameter = task_definition.task_param_defs[RESULT_PARAM]
            result_param_value = ParameterValue(
                parameter=parameter,
                source=task_definition.full_task_family_short,
                source_value=None,
                value=NOTHING,
                parsed=False,
            )
            param_values.append(result_param_value)

        return cls(
            task_name=task_name,
            task_definition=task_definition,
            task_params=Parameters(source="tracking_task", param_values=param_values),
        )

    @classmethod
    def for_user_params(cls, task_name, user_params, task_passport, source_code=None):
        # type: (Type[TrackingTask], str, Dict[str,Any], TaskPassport, TaskSourceCode) -> TrackingTask

        task_definition = TaskDefinition(
            task_passport=task_passport, source_code=source_code
        )
        param_values = []
        for key, value in six.iteritems(user_params):
            p = build_user_parameter_value(
                name=key, value=value, source=task_definition.full_task_family_short
            )
            param_values.append(p)
        task_params = Parameters(
            source=task_definition.full_task_family_short, param_values=param_values
        )
        return cls(
            task_name=task_name,
            task_definition=task_definition,
            task_params=task_params,
        )

    def __init__(self, task_name, task_definition, task_params):

        super(TrackingTask, self).__init__(
            task_name=task_name or task_definition.task_family,
            task_definition=task_definition,
            task_params=task_params,
        )
        self.task_definition = task_definition
        self.ctrl = TrackingTaskCtrl(self)

        # replace the appropriate parameters in the Task
        self.task_version = utcnow().strftime("%Y%m%d_%H%M%S")
        self.task_target_date = utcnow().date()
        self.task_env = get_databand_context().env

        self.task_call_source = [
            self.dbnd_context.user_code_detector.find_user_side_frame(1)
        ]

        self.task_outputs = dict()
        self.initialize_task_id(
            self.task_params.get_params_signatures(ParameterFilters.SIGNIFICANT_INPUTS)
        )

        for parameter, value in self._params.get_params_with_value(
            ParameterFilters.OUTPUTS
        ):
            if isinstance(parameter, FuncResultParameter):
                continue

            # This is used to keep backward compatibility for tracking luigi behaviour
            # This is not something we want to keep, at least not in this form
            value = traverse_and_set_target(value, parameter._target_source(self))
            self.task_outputs[parameter.name] = value

        out_params = self._params.get_params_with_value(ParameterFilters.OUTPUTS)
        self.initialize_task_output_id(out_params)
        self.ctrl._initialize_task()

        parent_task = try_get_current_task()
        if parent_task:
            parent_task.descendants.add_child(self.task_id)
            self.task_call_source.extend(parent_task.task_call_source)

        # so we can be found via task_id
        self.dbnd_context.task_instance_cache.register_task_instance(self)

    def get_task_family(self):
        return self.task_definition.task_family

    @property
    def tracker(self):
        return self.current_task_run.tracker

    @property
    def task_dag(self):
        return self.ctrl.task_dag

    @property
    def descendants(self):
        return self.ctrl.descendants

    @property
    def current_task_run(self):
        # type: ()->TaskRun
        return get_databand_run().get_task_run(self.task_id)

    @property
    def task_in_memory_outputs(self):
        return True

    @property
    @cached()
    def _meta_output(self):
        # in some sense this is a duplication of dbnd._core.task_ctrl.task_output_builder.calculate_path
        # but it also breaking an awful abstraction and a lot of inner functions which is good
        task_env = self.task_env
        sep = "/"
        root = no_trailing_slash(str(task_env.root))
        if windows_drive_re.match(root):
            sep = os.sep
        path = sep.join(
            (
                root,
                task_env.env_label,
                str(self.task_target_date),
                self.task_name,
                self.task_name + "_" + self.task_signature,
                "_meta_output",
                "meta",
            )
        )

        # meta_output is directory
        path += sep

        return target(path, folder)

    def _complete(self):
        return None


def build_func_parameter_values(task_definition, task_args, task_kwargs):
    """
    In tracking task we need to build params without definitions.
    Those params value need no calculations and therefore are very easy to construct
    """

    task_args, task_kwargs = args_to_kwargs(
        task_definition.func_spec.args, task_args, task_kwargs
    )

    args = ((str(i), value) for i, value in enumerate(task_args))
    kwargs = six.iteritems(task_kwargs)

    values = []
    for name, value in chain(args, kwargs):
        param_value = build_user_parameter_value(
            name, value, source=task_definition.full_task_family_short
        )
        values.append(param_value)

    return values
