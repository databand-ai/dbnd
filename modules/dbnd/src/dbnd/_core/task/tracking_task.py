# © Copyright Databand.ai, an IBM Company 2022

import logging
import typing

from typing import Any, Dict, List, Optional, Type
from uuid import UUID

import more_itertools
import six

from dbnd._core.constants import RESULT_PARAM, TaskEssence, _TaskParamContainer
from dbnd._core.current import get_databand_run, try_get_current_task
from dbnd._core.parameter import build_user_parameter_value
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.parameter.parameter_value import (
    ParameterFilters,
    Parameters,
    ParameterValue,
)
from dbnd._core.task.base_task import _BaseTask
from dbnd._core.task.task_mixin import _TaskCtrlMixin
from dbnd._core.task_build.task_definition import TaskDefinition
from dbnd._core.task_build.task_passport import TaskPassport
from dbnd._core.task_build.task_results import FuncResultParameter
from dbnd._core.task_build.task_signature import Signature, user_friendly_signature
from dbnd._core.task_build.task_source_code import TaskSourceCode
from dbnd._core.task_ctrl.task_ctrl import TrackingTaskCtrl
from dbnd._core.utils.basics.nothing import NOTHING, is_not_defined
from dbnd._core.utils.callable_spec import CallableSpec, args_to_kwargs
from dbnd._core.utils.timezone import utcnow
from dbnd._core.utils.uid_utils import get_uuid
from targets import InMemoryTarget
from targets.base_target import TargetSource


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun

logger = logging.getLogger(__name__)


def _generate_unique_tracking_signature():
    return Signature(
        "tracking", user_friendly_signature(str(get_uuid())), "unique tracking call"
    )


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
        """
        Creating the task for a decorated function.
        We use pre-created definition for the function definition.

        Here we create the tasks params from runtime input
        """
        param_values = build_func_parameter_values(
            task_definition, task_args, task_kwargs
        )
        # we need to add RESULT param
        if RESULT_PARAM in task_definition.task_param_defs:
            param = task_definition.task_param_defs[RESULT_PARAM]
            if isinstance(param, FuncResultParameter):
                for param_name in param.names:
                    # we want to get the parameter evolved with the task_definition as owner
                    inner_param = task_definition.task_param_defs[param_name]
                    result_param_value = build_result_param(
                        task_definition.task_passport, param_def=inner_param
                    )

                    param_values.append(result_param_value)

            result_param_value = build_result_param(
                task_definition.task_passport, param_def=param
            )
            param_values.append(result_param_value)

        task_params = Parameters(source="tracking_task", param_values=param_values)

        return cls(
            task_name=task_name or task_definition.task_family,
            task_definition=task_definition,
            task_params=task_params,
        )

    @classmethod
    def for_user_params(
        cls,
        task_definition_uid,  # type: UUID
        task_name,  # type: str
        user_params,  # type: Dict[str,Any]
        task_passport,  # type: TaskPassport
        source_code=None,  # type: TaskSourceCode
        result=True,  # type: Optional[bool]
    ):
        # type: (...) -> TrackingTask

        """
        Creating a customize task from the required params.
        Here we build the params from runtime values and use them to build the task definition.
        """
        param_values = []
        for key, value in six.iteritems(user_params):
            p = build_user_parameter_value(
                name=key, value=value, source=task_passport.full_task_family_short
            )
            param_values.append(p)

        if result:
            result_param_value = build_result_param(task_passport)
            param_values.append(result_param_value)

        task_params = Parameters(
            source=task_passport.full_task_family_short, param_values=param_values
        )

        task_definition = TaskDefinition(
            task_passport=task_passport,
            source_code=source_code,
            external_parameters=task_params,
            task_definition_uid=task_definition_uid,
        )

        return cls(
            task_name=task_name,
            task_definition=task_definition,
            task_params=task_params,
        )

    def __init__(
        self,
        task_name,
        task_definition,
        task_params,
        task_signature_obj=None,
        task_version=None,
    ):
        task_signature_obj = task_signature_obj or _generate_unique_tracking_signature()

        super(TrackingTask, self).__init__(
            task_name=task_name,
            task_definition=task_definition,
            task_signature_obj=task_signature_obj,
            task_params=task_params,
        )

        self.task_definition = task_definition  # type: TaskDefinition
        # we don't have signature for outputs
        self.task_outputs_signature_obj = self.task_signature_obj
        self.ctrl = TrackingTaskCtrl(self)

        self.task_call_source = [
            self.dbnd_context.user_code_detector.find_user_side_frame(4)
        ]
        parent_task = try_get_current_task()
        if parent_task:
            parent_task.descendants.add_child(self.task_id)
            self.task_call_source.extend(parent_task.task_call_source)

            # inherit from parent if it has it
            self.task_version = task_version or parent_task.task_version
            self.task_target_date = parent_task.task_target_date
            self.task_env = parent_task.task_env
            # pass-through parent children scope params
            # task_children_scope_params will be used in case of any Task inside TrackedTask
            # for example tracked task creates Config objects
            self.task_children_scope_params = parent_task.task_children_scope_params
        else:
            # we need better definition of "what we use for tracking"
            self.task_version = task_version or utcnow().strftime("%Y%m%d_%H%M%S")
            self.task_target_date = utcnow().date()
            self.task_env = None
            self.task_children_scope_params = {}

        self.task_outputs = dict()
        for parameter, value in self._params.get_params_with_value(
            ParameterFilters.OUTPUTS
        ):
            if is_not_defined(value):
                value_as_target = self.build_tracking_output(parameter)
                task_params.update_param_value(parameter.name, value_as_target)

            if isinstance(parameter, FuncResultParameter):
                continue

            # This is used to keep backward compatibility for tracking luigi behaviour
            # This is not something we want to keep, at least not in this form
            # from dbnd_run.task_ctrl.task_relations import traverse_and_set_target
            # value = traverse_and_set_target(value, parameter._target_source(self))

            self.task_outputs[parameter.name] = value

        self.ctrl._initialize_task()

        # so we can be found via task_id
        self.dbnd_context.task_instance_cache.register_task_instance(self)

    def build_tracking_output(self, p):
        return InMemoryTarget(
            path="memory://{value_type}:{task}.{p_name}".format(
                value_type=p.value_type, task=self.task_id, p_name=p.name
            ),
            source=TargetSource(task_id=self.task_id, parameter_name=p.name),
        )

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

    def _complete(self):
        return None


def build_result_param(task_passport, param_def=None, name=RESULT_PARAM):
    # type: (TaskPassport, Optional[ParameterDefinition], str) -> ParameterValue
    """
    Build results parameter for the task definition, if parameter definition is not specify it will build a naive one.
    """

    if not param_def:
        from targets.values import ObjectValueType

        # naive creation of result param definition - default named "result" and single value
        param_def = parameter.modify(
            name=name, value_type=ObjectValueType
        ).output.build_parameter("inline")

    return ParameterValue(
        parameter=param_def,
        source=task_passport.full_task_family_short,
        source_value=None,
        value=NOTHING,
        parsed=False,
    )


def build_func_parameter_values(task_definition, task_args, task_kwargs):
    # type: (TaskDefinition, List[Any], Dict[str, Any]) -> List[ParameterValue]
    """
    In tracking task we need to build params without definitions.
    Those params value need no calculations and therefore are very easy to construct
    """
    callable_spec = (
        task_definition.task_decorator.get_callable_spec()
    )  # type: CallableSpec
    values = []

    # convert any arg to kwarg if possible
    args, task_kwargs = args_to_kwargs(callable_spec.args, task_args, task_kwargs)

    # the parameter of the * argument
    if callable_spec.varargs:
        # build the parameter value for the varargs
        vargs_param = build_user_parameter_value(
            callable_spec.varargs,
            tuple(args),
            source=task_definition.full_task_family_short,
        )
        values.append(vargs_param)

    # distinguish between the parameter we expect to create vs those we don't
    unknown_kwargs, known_kwargs = more_itertools.partition(
        lambda kwarg: kwarg[0] in callable_spec.args, six.iteritems(task_kwargs)
    )

    # the parameter of the ** argument
    if callable_spec.varkw:
        # build the parameter value for the varkw
        varkw_param = build_user_parameter_value(
            callable_spec.varkw,
            dict(unknown_kwargs),
            source=task_definition.full_task_family_short,
        )
        values.append(varkw_param)

    # Exclude the self param just like it's excluded in DecoratedCallableParamBuilder
    excluded_params = {"self"}

    for name, value in known_kwargs:
        if name in excluded_params:
            continue

        # build the parameters for the expected parameters
        param_value = build_user_parameter_value(
            name, value, source=task_definition.full_task_family_short
        )
        values.append(param_value)

    return values
