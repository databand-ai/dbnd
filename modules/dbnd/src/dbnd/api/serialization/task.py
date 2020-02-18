from dbnd._core.constants import TaskRunState
from dbnd._core.parameter.parameter_definition import ParameterGroup, _ParameterKind
from dbnd._core.tracking.tracking_info_objects import (
    TaskDefinitionInfo,
    TaskRunInfo,
    TaskRunParamInfo,
)
from dbnd._vendor.marshmallow import fields, post_load
from dbnd._vendor.marshmallow_enum import EnumField
from dbnd.api.api_utils import ApiObjectSchema, _as_dotted_dict


class TaskDefinitionParamSchema(ApiObjectSchema):
    """
    Based on TaskDefinitionParam object
    """

    name = fields.String()

    default = fields.String(allow_none=True)

    description = fields.String()

    group = EnumField(ParameterGroup)
    kind = EnumField(_ParameterKind)
    load_on_build = fields.Boolean()

    significant = fields.Boolean()
    value_type = fields.String()

    @post_load
    def make_task_definition_param(self, data, **kwargs):
        return _as_dotted_dict(**data)


class TaskDefinitionInfoSchema(ApiObjectSchema):
    task_definition_uid = fields.UUID()
    name = fields.String()

    class_version = fields.String()
    family = fields.String()

    module_source = fields.String(allow_none=True)
    module_source_hash = fields.String(allow_none=True)

    source = fields.String(allow_none=True)
    source_hash = fields.String(allow_none=True)

    type = fields.String()

    task_param_definitions = fields.Nested(TaskDefinitionParamSchema, many=True)

    @post_load
    def make_task_definition(self, data, **kwargs):
        return TaskDefinitionInfo(**data)


class TaskRunParamSchema(ApiObjectSchema):
    parameter_name = fields.String()
    value_origin = fields.String()
    value = fields.String()

    @post_load
    def make_task_run_param(self, data, **kwargs):
        return TaskRunParamInfo(**data)


class TaskRunInfoSchema(ApiObjectSchema):
    task_run_uid = fields.UUID()
    task_run_attempt_uid = fields.UUID()

    task_definition_uid = fields.UUID()
    run_uid = fields.UUID()
    task_id = fields.String()
    task_signature = fields.String()
    task_signature_source = fields.String()

    task_af_id = fields.String()
    execution_date = fields.DateTime()

    name = fields.String()

    env = fields.String()

    command_line = fields.String()
    functional_call = fields.String()

    has_downstreams = fields.Boolean()
    has_upstreams = fields.Boolean()

    is_reused = fields.Boolean()
    is_dynamic = fields.Boolean()
    is_system = fields.Boolean()
    is_skipped = fields.Boolean()
    is_root = fields.Boolean()
    output_signature = fields.String()

    state = EnumField(TaskRunState)
    target_date = fields.Date(allow_none=True)

    log_local = fields.String(allow_none=True)
    log_remote = fields.String(allow_none=True)

    version = fields.String()

    task_run_params = fields.Nested(TaskRunParamSchema, many=True)

    @post_load
    def make_task_run(self, data, **kwargs):
        return TaskRunInfo(**data)
