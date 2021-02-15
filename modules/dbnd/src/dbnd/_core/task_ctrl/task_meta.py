import logging

from typing import TYPE_CHECKING, Dict, List

import six

from dbnd._core.configuration.dbnd_config import _ConfigLayer, config
from dbnd._core.current import get_databand_context, try_get_current_task
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
    from dbnd._core.task_build.task_definition import TaskDefinition
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
        task_enabled=True,
        task_sections=None,
    ):
        super(TaskMeta, self).__init__()
        # we should not use it directly, the value in object can outdated
        self.task_params = task_params  # type: Dict[str, ParameterValue]

        # passport
        self.task_name = task_name  # type: str
        self.task_family = task_family  # type: str
        self.task_definition = task_definition  # type: TaskDefinition

        # configuration data
        self.config_layer = config_layer  # type: _ConfigLayer
        self.task_sections = task_sections
        self.task_config_override = task_config_override

        self.dbnd_context = get_databand_context()  # type: DatabandContext

        # ids and signatures
        self.task_id = None
        self.task_signature = None
        self.task_signature_source = None
        self.task_outputs_signature = None
        self.task_outputs_signature_source = None

        self.task_enabled = task_enabled

        self.obj_key = self._calculate_task_meta_key()
        # we want to have task id immediately
        self.initialize_task_id(
            [
                (p_value.name, p_value.parameter.signature(p_value.value))
                for p_value in self.task_params.values()
                if isinstance(p_value.parameter, ParameterDefinition)
                and p_value.parameter.significant
            ]
        )

        self.task_enabled = task_enabled  # relevant only for orchestration

        # miscellaneous
        self.task_type = self.task_definition.task_class._conf__task_type_name
        self.task_user = username

        self.task_call_source = [
            self.dbnd_context.user_code_detector.find_user_side_frame(2)
        ]
        parent_task = try_get_current_task()
        if self.task_call_source and parent_task:
            self.task_call_source.extend(parent_task.task_meta.task_call_source)

        # we count task meta creation
        # even if task_meta will not be used by TaskMetaClass when we already have created task
        # we will increase creation id
        # if t2 created after t1, t2.task_meta.task_creation_id > t1.task_meta.task_creation_id
        TaskMeta._current_task_creation_id += 1
        self.task_creation_id = TaskMeta._current_task_creation_id

    def _calculate_task_meta_key(self):
        params = [
            (p_value.name, p_value.parameter.signature(p_value.value))
            for p_value in self.task_params.values()
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

    def get_task_config_value(self, key):
        for section in self.task_sections:
            config_value = config.get_config_value(section, key)
            if config_value:
                return config_value
        return None
