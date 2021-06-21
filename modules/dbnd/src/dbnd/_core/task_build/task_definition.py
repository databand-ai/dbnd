import logging
import typing

from collections import OrderedDict
from typing import Any, Dict, List, Optional
from uuid import UUID

import six

from six import iteritems

from dbnd._core.configuration.config_readers import parse_and_build_config_store
from dbnd._core.configuration.config_value import ConfigValuePriority
from dbnd._core.configuration.dbnd_config import config
from dbnd._core.constants import RESULT_PARAM
from dbnd._core.parameter.parameter_builder import ParameterFactory
from dbnd._core.parameter.parameter_definition import (
    ParameterDefinition,
    _ParameterKind,
)
from dbnd._core.parameter.parameter_value import Parameters
from dbnd._core.task_build.decorated_callable_param_builder import (
    DecoratedCallableParamBuilder,
)
from dbnd._core.task_build.task_passport import TaskPassport
from dbnd._core.task_build.task_results import FuncResultParameter
from dbnd._core.task_build.task_signature import user_friendly_signature
from dbnd._core.task_build.task_source_code import TaskSourceCode
from dbnd._core.utils.basics.nothing import is_defined
from dbnd._core.utils.structures import combine_mappings
from dbnd._core.utils.uid_utils import get_uuid


if typing.TYPE_CHECKING:
    from dbnd._core.task_build.task_decorator import TaskDecorator

logger = logging.getLogger(__name__)


def _ordered_params(x):
    return OrderedDict(sorted(x.items(), key=lambda y: y[1].parameter_id))


class TaskDefinition(object):
    """
    TaskDefinition contains all the information gathered and calculated, previous to the task creation.
    """

    @classmethod
    def from_task_cls(cls, task_class, classdict):
        """
        Creating the task definition for a task defined as a class or orchestration decorated task
        This is getting called from the creation of the class itself (by the meta-class)
        """

        # collecting the definitions of the inherited task classes (if any).
        base_task_definitions = get_base_task_definitions(task_class)

        return TaskDefinition(
            classdict=classdict,
            base_task_definitions=base_task_definitions,
            task_passport=TaskPassport.from_task_cls(task_class),
            defaults=classdict.get("defaults", None),
            task_decorator=task_class.task_decorator,
            source_code=TaskSourceCode.from_task_class(task_class),
        )

    @classmethod
    def from_task_decorator(cls, task_decorator):
        # type: (TaskDecorator) -> TaskDefinition
        """
        Creating the task definition from a decorated function.
        This requires the information collected on the function signature
        """
        return TaskDefinition(
            task_passport=task_decorator.task_passport,
            defaults=task_decorator.task_defaults,
            task_decorator=task_decorator,
            source_code=TaskSourceCode.from_callable(
                task_decorator.original_class_or_func
            ),
        )

    def __init__(
        self,
        task_passport,  # type: TaskPassport
        classdict=None,  # type: Optional[Dict[str, Any]]
        base_task_definitions=None,  # type: Optional[List[TaskDefinition]]
        defaults=None,  # type: Optional[Dict[ParameterDefinition, Any]]
        task_decorator=None,  # type: Optional[TaskDecorator]
        source_code=None,  # type: Optional[TaskSourceCode]
        external_parameters=None,  # type: Optional[Parameters]
        task_definition_uid=None,  # type: Optional[UUID]
    ):
        super(TaskDefinition, self).__init__()

        self.hidden = False

        self.task_passport = task_passport
        self.source_code = source_code
        self.task_decorator = task_decorator
        self.base_task_definitions = (
            base_task_definitions or []
        )  # type: List[ TaskDefinition]

        # TODO: maybe use properties or other way to delegate those...
        self.full_task_family = self.task_passport.full_task_family
        self.full_task_family_short = self.task_passport.full_task_family_short
        self.task_family = self.task_passport.task_family
        self.task_config_section = self.task_passport.task_config_section

        # all the attributes that points to_Parameter
        self.task_param_defs = dict()  # type: Dict[str, ParameterDefinition]

        # the defaults attribute
        self.defaults = dict()  # type: Dict[ParameterDefinition, Any]

        self.task_param_defs = self._calculate_task_class_values(
            classdict, external_parameters
        )
        # if we have output params in function arguments, like   f(some_p=parameter.output)
        # the new function can not return the result of return
        self.single_result_output = self._is_result_single_output(self.task_param_defs)

        self.param_defaults = {
            p.name: p.default
            for p in self.task_param_defs.values()
            if is_defined(p.default)
        }

        # TODO: consider joining with task_config
        # TODO: calculate defaults value as _ConfigStore and merge using standard mechanism
        self.defaults = self._calculate_task_defaults(defaults)
        self.task_defaults_config_store = parse_and_build_config_store(
            source=self.task_passport.format_source_name("task.defaults"),
            config_values=self.defaults,
            priority=ConfigValuePriority.FALLBACK,
        )

        self.task_signature_extra = {}
        if config.getboolean("task_build", "sign_with_full_qualified_name"):
            self.task_signature_extra["full_task_family"] = self.full_task_family
        if config.getboolean("task_build", "sign_with_task_code"):
            self.task_signature_extra["task_code_hash"] = user_friendly_signature(
                self.source_code.task_source_code
            )

        if task_definition_uid:
            self.task_definition_uid = task_definition_uid
        else:
            self.task_definition_uid = get_uuid()

    def _calculate_task_class_values(self, classdict, external_parameters):
        # type: (Optional[Dict],  Optional[Parameters]) -> Dict[str, ParameterDefinition]
        # reflect inherited attributes
        params = dict()
        # params will contain definition of param, even it's was overrided by the parent task
        for base_schema in self.base_task_definitions:
            params = combine_mappings(params, base_schema.task_param_defs)

        # let update params with new class attributes
        self._update_params_from_attributes(classdict, params)

        # this is the place we add parameters from function definition
        if self.task_decorator is not None:
            func_params_builder = DecoratedCallableParamBuilder(
                base_params=params, task_decorator=self.task_decorator
            )

            func_params_builder.build_func_params()
            params_dict = dict(func_params_builder.decorator_kwargs_params)
            params_dict.update(func_params_builder.func_spec_params)
            params_dict.update(func_params_builder.result_params)

            self._update_params_from_attributes(params_dict, params)

        if external_parameters:
            params.update(
                {param.name: param for param in external_parameters.get_params()}
            )

        updated_params = {}
        for name, param in six.iteritems(params):
            # add parameters config
            param_with_owner = param.evolve_with_owner(task_definition=self, name=name)

            # updated the owner in the external parameters
            param_value = external_parameters and external_parameters.get_param_value(
                name
            )
            if param_value:
                param_value.parameter = param_with_owner

            updated_params[name] = param_with_owner

        params = _ordered_params(updated_params)
        return params

    def _calculate_task_defaults(self, defaults):
        # type: (...)->  Dict[str, Any]
        base_defaults = dict()
        for base_schema in self.base_task_definitions:
            base_defaults = combine_mappings(base_defaults, base_schema.defaults)

        return combine_mappings(base_defaults, defaults)

    def _update_params_from_attributes(self, classdict, params):
        class_values = dict()
        if not classdict:
            return

        for a_name, a_obj in iteritems(classdict):
            context = "%s.%s" % (self.task_family, a_name)
            try:
                if isinstance(a_obj, ParameterFactory):
                    params[a_name] = a_obj.build_parameter(context)
                elif isinstance(a_obj, ParameterDefinition):
                    params[a_name] = a_obj
                else:
                    class_values[a_name] = a_obj
            except Exception:
                logger.warning("Failed to process %s" % context)
                raise

        # now, if we have overloads in code ( calculated in task_definition):
        # class T(BaseT):
        #     some_base_t_property = new_value
        for p_name, p_val in iteritems(class_values):
            if p_name not in params:
                continue
            params[p_name] = params[p_name].modify(default=p_val)

    def _is_result_single_output(self, params):
        """
        check that task has only one output and it's output is result
         (there can be sub outputs that are part of result)
        """
        result = params.get(RESULT_PARAM)
        if not result:
            return False
        names = result.names if isinstance(result, FuncResultParameter) else []
        for param_def in self.task_param_defs.values():
            if param_def.system or param_def.kind != _ParameterKind.task_output:
                continue
            if param_def.name in [RESULT_PARAM, "task_band"]:
                continue
            if param_def.name in names:
                continue
            return False
        return True

    def __str__(self):
        return "TaskDefinition(%s)" % self.full_task_family

    def run_name(self):
        if self.task_decorator:
            return "%s()" % self.full_task_family
        return "%s.run()" % self.full_task_family


def get_base_task_definitions(task_class):
    """Accessing the task_definitions of the parents classes of the given `task_class`"""
    task_definitions = []

    for cls in reversed(task_class.__bases__):  # type: TaskMetaclass
        if not hasattr(cls, "task_definition"):
            logger.debug(
                "you should inherit from Task objects only: %s -> %s ", task_class, cls,
            )
            continue
        task_definitions.append(cls.task_definition)
    return task_definitions
