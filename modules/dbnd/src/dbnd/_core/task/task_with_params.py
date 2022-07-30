# © Copyright Databand.ai, an IBM Company 2022

from typing import TYPE_CHECKING, Any, Dict

import six

from dbnd._core.constants import ParamValidation
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task.base_task import _BaseTask
from dbnd._core.task_build.task_metaclass import TaskMetaclass


if TYPE_CHECKING:
    from dbnd._core.parameter.parameter_definition import ParameterDefinition
    from dbnd._core.parameter.parameter_value import Parameters, ParameterValue
    from dbnd._core.task_build.task_definition import TaskDefinition


@six.add_metaclass(TaskMetaclass)
class _TaskWithParams(_BaseTask):
    _conf__scoped_params = True  # enable/disable ParameterScope.CHILDREN scope params
    _conf_auto_read_params = True  # enables auto-read value of data params.

    def __init__(
        self,
        task_name,
        task_definition,
        task_signature_obj,
        task_params,  # type: Parameters
        task_config_layer,
        task_config_override,
        task_enabled=True,
        task_sections=None,
        task_call_source=None,
        task_children_scope_params=None,  # type: Dict[str, ParameterValue]
    ):
        super(_TaskWithParams, self).__init__(
            task_name=task_name,
            task_definition=task_definition,
            task_signature_obj=task_signature_obj,
            task_params=task_params,
        )

        self.task_enabled = task_enabled  # relevant only for orchestration

        # configuration data
        self.task_sections = task_sections
        self.task_config_override = task_config_override
        self.task_config_layer = task_config_layer

        self.task_children_scope_params = task_children_scope_params

        self.task_call_source = task_call_source

        for param_value in self.task_params.get_param_values():
            param_value.task = self
            object.__setattr__(self, param_value.name, param_value.value)

    task_definition = None  # type: TaskDefinition

    # user can override this with his configuration
    defaults = None  # type: Dict[ParameterDefinition, Any]

    validate_no_extra_params = parameter.enum(ParamValidation).system(
        default=ParamValidation.disabled,
        description="validate that all configured keys for a task have a matching parameter definition",
    )

    def __setattr__(self, name, value):
        p = (
            self.task_params.get_param_value(name)
            if hasattr(self, "task_params")
            else None
        )
        if p:
            p._update_param_value_from_task_set(value)

        object.__setattr__(self, name, value)

    def _update_property_on_parameter_value_set(self, name, value):
        """called from param.update_value() , we don't want to call back p._update_param_value_from_task_set()"""
        object.__setattr__(self, name, value)

    def _initialize(self):
        pass

    def _validate(self):
        """
        will be called after after object is created
        :return:
        """
        return

    def clone(self, cls=None, output_params_to_clone=None, **kwargs):
        """
        Creates a new instance from an existing instance where some of the args have changed.

        There's at least two scenarios where this is useful (see test/clone_test.py):

        * remove a lot of boiler plate when you have recursive dependencies and lots of args
        * there's task inheritance and some logic is on the base class

        :param cls:
        :param kwargs:
        :return:
        """
        if cls is None:
            cls = self.__class__
        output_params_to_clone = output_params_to_clone or []
        new_k = {}

        # copy only fields that exists in target class
        for param_name, param_class in six.iteritems(
            cls.task_definition.task_param_defs
        ):
            if param_class.is_output() and param_name not in output_params_to_clone:
                continue
            if hasattr(self, param_name):
                new_k[param_name] = getattr(self, param_name)
        new_k["task_name"] = self.task_name

        new_k.update(kwargs)
        return cls(**new_k)

    @classmethod
    def get_task_family(cls):
        return cls.task_definition.task_family

    @classmethod
    def get_full_task_family(cls):
        return cls.task_definition.full_task_family
