# -*- coding: utf-8 -*-
#
# Copyright 2015 Spotify AB
# Modifications copyright (C) 2018 databand.ai
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
The abstract :py:class:`Task` class.
It is a central concept of databand and represents the state of the workflow.
See :doc:`/tasks` for an overview.
"""

import logging

from typing import TYPE_CHECKING, Any, Dict, Optional

import six

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.constants import ParamValidation, TaskType
from dbnd._core.current import get_databand_context
from dbnd._core.decorator.task_decorator_spec import _TaskDecoratorSpec
from dbnd._core.errors import friendly_error
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.parameter.parameter_value import ParameterFilters, Parameters
from dbnd._core.task_build.task_definition import TaskDefinition
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd._core.task_build.task_signature import (
    build_signature,
    build_signature_from_values,
    user_friendly_signature,
)
from dbnd._core.utils.basics.nothing import NOTHING


if TYPE_CHECKING:
    from dbnd._core.task_build.task_definition import TaskDefinition
    from dbnd._core.settings import DatabandSettings
    from dbnd._core.context.databand_context import DatabandContext

logger = logging.getLogger(__name__)


class _BaseTask(object):
    _current_task_creation_id = 0

    # override to change get_task_family() -> changes task_family
    _conf__task_family = None
    _conf__track_source_code = True  # module/class/function user source code tracking
    task_namespace = NOTHING

    _conf__task_type_name = TaskType.python
    _conf__task_ui_color = None
    #####
    # override output path format
    _conf__base_output_path_fmt = None

    # stores call spec for the @task definition
    _conf__decorator_spec = None  # type: Optional[_TaskDecoratorSpec]
    _conf__tracked = True  # track task changes with TrackingStore

    @property
    def task_family(self):
        return self.task_definition.task_passport.task_family

    @property
    def _params(self):
        return self.task_params

    def __init__(
        self, task_name, task_definition, task_params,  # type: Parameters
    ):

        super(_BaseTask, self).__init__()
        # we should not use it directly, the value in object can outdated
        self.task_params = task_params
        self.task_name = task_name  # type: str

        # ids and signatures
        self.task_id = None
        self.task_signature = None
        self.task_signature_source = None
        self.task_outputs_signature = None
        self.task_outputs_signature_source = None

        # define it at the time of creation
        # we can remove the strong reference, the moment we have global cache for instances
        # otherwise if we moved to another databand_context, task_id relationships will not be found
        self.dbnd_context = get_databand_context()

        # we want to have task id immediately
        params_for_id = self.task_params.get_params_signatures(
            param_filter=ParameterFilters.SIGNIFICANT_ONLY
        )
        self.initialize_task_id(params_for_id)

        # miscellaneous
        self.task_type = self._conf__task_type_name

        # we count task meta creation
        # even if cached task is going to be used we will increase creation id
        # if t2 created after t1, t2.task_creation_id > t1.task_creation_id
        _BaseTask._current_task_creation_id += 1
        self.task_creation_id = _BaseTask._current_task_creation_id

    def initialize_task_id(self, params=None):
        name = self.task_name
        extra = {}
        if config.getboolean("task_build", "sign_with_full_qualified_name"):
            extra["full_task_family"] = self.task_definition.full_task_family
        if config.getboolean("task_build", "sign_with_task_code"):
            extra["task_code_hash"] = user_friendly_signature(
                self.task_definition.task_source_code
            )

        signature = build_signature(name=name, params=params, extra=extra)
        self.task_id = signature.id
        self.task_signature = signature.signature
        self.task_signature_source = signature.signature_source

    def initialize_task_output_id(self, task_outputs):
        if task_outputs:
            sig = build_signature_from_values("task_outputs", task_outputs)
            self.task_outputs_signature = sig.signature
            self.task_outputs_signature_source = sig.signature_source
        else:
            self.task_outputs_signature = self.task_signature
            self.task_outputs_signature_source = self.task_signature_source

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.task_id == other.task_id

    def __hash__(self):
        return hash(self.task_id)

    def __repr__(self):
        """
        Build a task representation like `MyTask(param1=1.5, param2='5')`
        """
        return "%s" % self.task_id

    def __str__(self):
        """
        Build a task representation like `MyTask(param1=1.5, param2='5')`
        """
        return self.task_id

    @property
    def friendly_task_name(self):
        if self.task_name != self.task_family:
            return "%s[%s]" % (self.task_name, self.task_family)
        return self.task_name

    @property
    def settings(self):
        # type: () -> DatabandSettings

        return self.dbnd_context.settings

    def __iter__(self):
        raise friendly_error.task_build.iteration_over_task(self)

    def _task_banner(self, banner, verbosity):
        """
        customize task banner
        """
        return


@six.add_metaclass(TaskMetaclass)
class _TaskWithParams(_BaseTask):
    _conf__no_child_params = False  # disable child scope params
    _conf_auto_read_params = True  # enables autoread of params.

    def __init__(
        self,
        task_name,
        task_definition,
        task_params,  # type: Parameters
        task_config_layer,
        task_config_override,
        task_enabled=True,
        task_sections=None,
    ):
        super(_TaskWithParams, self).__init__(
            task_name=task_name,
            task_definition=task_definition,
            task_params=task_params,
        )

        self.task_enabled = task_enabled  # relevant only for orchestration

        # configuration data
        self.task_sections = task_sections
        self.task_config_override = task_config_override
        self.task_config_layer = task_config_layer

        for param_value in self.task_params.get_param_values():
            param_value.task = self
            object.__setattr__(self, param_value.name, param_value.value)

        ####
        # execution
        # will be used by Registry

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
        """ called from param.update_value() , we don't want to call back p._update_param_value_from_task_set()"""
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
        for param_name, param_class in six.iteritems(
            cls.task_definition.task_param_defs
        ):
            if param_class.is_output() and param_name not in output_params_to_clone:
                continue
            if param_name in kwargs:
                new_k[param_name] = kwargs[param_name]
            elif hasattr(self, param_name):
                new_k[param_name] = getattr(self, param_name)

        new_k["task_name"] = self.task_name
        return cls(**new_k)

    @classmethod
    def get_task_family(cls):
        return cls.task_definition.task_family

    @classmethod
    def get_full_task_family(cls):
        return cls.task_definition.full_task_family
