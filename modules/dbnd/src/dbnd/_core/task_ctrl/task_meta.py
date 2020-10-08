import logging

from typing import TYPE_CHECKING, List

import six

from dbnd._core.configuration.dbnd_config import _ConfigLayer, config
from dbnd._core.errors import DatabandError
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.parameter.parameter_value import ParameterValue
from dbnd._core.task_build.task_signature import (
    Signature,
    build_signature,
    build_signature_from_values,
    user_friendly_signature,
)
from dbnd._core.utils.platform.windows_compatible.getuser import dbnd_getuser


if TYPE_CHECKING:
    from dbnd import Task
    from dbnd._core.task_build.task_definition import TaskDefinition
    from dbnd._core.task_build.task_params import TaskValueParams
    from dbnd._core.context.databand_context import DatabandContext

logger = logging.getLogger(__name__)

username = dbnd_getuser()


class TaskMeta(object):
    _current_task_creation_id = 0

    def __init__(
        self,
        task_name,
        task_family,
        task_definition,
        config_layer,
        task_params,
        task_config_override,
        dbnd_context,
        task_enabled=True,
        build_warnings=None,
        task_sections=None,
        task_call_source=None,
    ):
        super(TaskMeta, self).__init__()
        # we should not use it directly, the value in object can outdated
        self._task_params = task_params  # type: TaskValueParams
        self.class_task_params = list(
            task_params.class_params.values()
        )  # type: List[ParameterValue]

        self.config_layer = config_layer  # type: _ConfigLayer
        self.task_name = task_name  # type: str
        self.task_family = task_family  # type: str
        self.task_definition = task_definition  # type: TaskDefinition

        self.task_sections = task_sections
        self.task_config_override = task_config_override
        self.dbnd_context = dbnd_context  # type: DatabandContext

        self.children = set()
        self.build_warnings = build_warnings or []

        self.task_id = None
        self.task_signature = None
        self.task_signature_source = None

        self.task_outputs_signature = None
        self.task_outputs_signature_source = None

        self.task_enabled = task_enabled

        self.task_call_source = task_call_source

        sign_task_params = list(
            task_params.all_params.values()
        )  # type: List[ParameterValue]
        self.obj_key = self._calculate_task_meta_key(sign_task_params)
        # we want to have task id immediately
        self.initialize_task_id(
            [
                (p_value.name, p_value.parameter.signature(p_value.value))
                for p_value in sign_task_params
                if isinstance(p_value.parameter, ParameterDefinition)
                and p_value.parameter.significant
            ]
        )

        self.dag_id = self.task_name
        self.task_type = self.task_definition.task_class._conf__task_type_name

        self.task_command_line = None
        self.task_functional_call = None
        self.task_user = username

        # we count task meta creation
        # even if task_meta will not be used by TaskMetaClass when we already have created task
        # we will increase creation id
        # if t2 created after t1, t2.task_meta.task_creation_id > t1.task_meta.task_creation_id
        TaskMeta._current_task_creation_id += 1
        self.task_creation_id = TaskMeta._current_task_creation_id

    def add_child(self, task_id):
        self.children.add(task_id)

    def get_children(self):
        # type: (...)-> List[Task]
        tic = self.dbnd_context.task_instance_cache
        children = []
        for c_id in self.children:
            child_task = tic.get_task_by_id(c_id)
            if child_task is None:
                raise DatabandError(
                    "You have created %s in different dbnd_context, "
                    "can't find task object in current context!" % c_id
                )
            children.append(child_task)
        return children

    def _calculate_task_meta_key(self, sign_task_params):
        # type: (List[ParameterValue]) -> Signature
        params = [
            (p_value.name, p_value.parameter.signature(p_value.value))
            for p_value in sign_task_params
            if not p_value.parameter.is_output() and p_value.parameter.significant
        ]
        override_signature = {}
        for p_obj, p_val in six.iteritems(self.task_config_override):
            if isinstance(p_obj, ParameterDefinition):
                override_key = "%s.%s" % (p_obj.task_cls.get_task_family(), p_obj.name)
                override_value = (
                    p_val
                    if isinstance(p_val, six.string_types)
                    else p_obj.signature(p_val)
                )
            else:
                # very problematic approach till we fix the override structure
                override_key = str(p_obj)
                override_value = str(p_val)
            override_signature[override_key] = override_value

        params.append(("task_override", override_signature))

        # task schema id is unique per Class definition.
        # so if we have new implementation - we will not a problem with rerunning it
        full_task_name = "%s@%s(object=%s)" % (
            self.task_name,
            self.task_definition.full_task_family,
            str(id(self.task_definition)),
        )

        return build_signature(name=full_task_name, params=params)

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
        self.task_id, self.task_signature = (signature.id, signature.signature)

        self.task_signature_source = signature.signature_source

    def initialize_task_output_id(self, task_outputs):
        if task_outputs:
            sig = build_signature_from_values("task_outputs", task_outputs)
            self.task_outputs_signature = sig.signature
            self.task_outputs_signature_source = sig.signature_source
        else:
            self.task_outputs_signature = self.task_signature
            self.task_outputs_signature_source = self.task_signature_source

    def get_task_config_value(self, key):
        for section in self.task_sections:
            config_value = config.get_config_value(section, key)
            if config_value:
                return config_value
        return None
