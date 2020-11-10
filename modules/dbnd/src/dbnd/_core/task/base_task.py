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
import typing

from typing import Dict, Optional

import six

from dbnd._core.constants import ParamValidation, TaskType
from dbnd._core.decorator.lazy_object_proxy import CallableLazyObjectProxy
from dbnd._core.decorator.task_decorator_spec import _TaskDecoratorSpec
from dbnd._core.errors import friendly_error
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.task_build.task_definition import TaskDefinition
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd._core.task_ctrl.task_auto_values import TaskAutoParamsReadWrite
from dbnd._core.task_ctrl.task_meta import TaskMeta
from dbnd._core.task_ctrl.task_parameters import TaskParameters
from dbnd._core.utils.basics.nothing import NOTHING


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd._core.settings import DatabandSettings


@six.add_metaclass(TaskMetaclass)
class _BaseTask(object):
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
    _conf__no_child_params = False  # disable child scope params
    _conf_auto_read_params = True  # enables autoread of params.
    _conf_confirm_on_kill_msg = None  # get user confirmation on task kill if not empty
    _conf__require_run_dump_file = False

    # this is the state of autoread
    _task_auto_read_original = None
    ####
    # execution
    # will be used by Registry
    task_definition = None  # type: TaskDefinition
    is_tracking_mode = False  # type: bool

    # user can override this with his configuration
    defaults = None  # type: Dict[ParameterDefinition, any()]

    validate_no_extra_params = parameter.enum(ParamValidation).system(
        description="validate that all configured keys for a task have a matching parameter definition"
    )

    @classmethod
    def get_task_family(cls):
        return cls.task_definition.task_family

    @classmethod
    def get_full_task_family(cls):
        return cls.task_definition.full_task_family

    def __init__(self, **kwargs):
        super(_BaseTask, self).__init__()

        # most of the time we will use it as TaskMetaCtrl - we want this to be type hint!
        self.task_meta = kwargs["task_meta"]  # type: TaskMeta
        self._params = TaskParameters(self)

        for p_value in self.task_meta.class_task_params:
            setattr(self, p_value.name, p_value.value)

        self._task_auto_read_current = None
        self._task_auto_read_origin = None

    @property
    def task_id(self):
        return self.task_meta.task_id

    @property
    def task_signature(self):
        return self.task_meta.task_signature

    @property
    def task_name(self):
        return self.task_meta.task_name

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self._params.get_param_values() == other._params.get_param_values()
        )

    def __hash__(self):
        return hash(self.task_id)

    def __repr__(self):
        """
        Build a task representation like `MyTask(param1=1.5, param2='5')`
        """
        return "%s" % self.task_meta.task_id

    def __str__(self):
        """
        Build a task representation like `MyTask(param1=1.5, param2='5')`
        """
        return self.task_meta.task_id

    @property
    def friendly_task_name(self):
        if self.task_name != self.task_meta.task_family:
            return "%s[%s]" % (self.task_name, self.task_meta.task_family)
        return self.task_name

    def _auto_load_save_params(
        self, auto_read=False, save_on_change=False, normalize_on_change=False
    ):
        c = TaskAutoParamsReadWrite(
            task=self,
            auto_read=auto_read,
            save_on_change=save_on_change,
            normalize_on_change=normalize_on_change,
        )
        return c.auto_load_save_params()

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
            cls.task_definition.all_task_params
        ):
            if param_class.is_output() and param_name not in output_params_to_clone:
                continue
            if param_name in kwargs:
                new_k[param_name] = kwargs[param_name]
            elif hasattr(self, param_name):
                new_k[param_name] = getattr(self, param_name)

        new_k["task_name"] = self.task_name
        return cls(**new_k)

    @property
    def settings(self):
        # type: () -> DatabandSettings

        return self.task_meta.dbnd_context.settings

    def __iter__(self):
        raise friendly_error.task_build.iteration_over_task(self)

    def _initialize(self):
        pass

    def _task_banner(self, banner, verbosity):
        """
        customize task banner
        """
        return

    def _validate(self):
        """
        will be called after after object is created
        :return:
        """
        return

    def simple_params_dict(self):
        return {p.name: p.value for p in self._params.task_meta.class_task_params}

    def load_task_runtime_values(self):
        for param, value in self._params.get_param_values():
            runtime_value = param.calc_runtime_value(value, task=self)
            setattr(self, param.name, runtime_value)
