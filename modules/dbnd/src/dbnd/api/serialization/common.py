from dbnd._core.constants import TaskRunState
from dbnd._core.tracking.schemas.base import ApiObjectSchema
from dbnd._core.tracking.schemas.metrics import Metric
from dbnd._core.tracking.schemas.tracking_info_objects import ErrorInfo, TargetInfo
from dbnd._vendor.marshmallow import fields, post_load
from dbnd._vendor.marshmallow_enum import EnumField


class TargetInfoSchema(ApiObjectSchema):
    parameter_name = fields.String()

    path = fields.String()
    created_date = fields.DateTime(allow_none=True)
    task_run_uid = fields.UUID(allow_none=True)

    @post_load
    def make_target(self, data, **kwargs):
        return TargetInfo(**data)


class MetricSchema(ApiObjectSchema):
    key = fields.String()
    value = fields.Raw(allow_none=True)
    value_str = fields.String(allow_none=True)
    value_json = fields.Dict(allow_none=True)
    value_int = fields.Integer(allow_none=True)
    value_float = fields.Float(allow_none=True)
    timestamp = fields.DateTime()

    @post_load
    def make_object(self, data, **kwargs):
        return Metric(**data)


class ArtifactSchema(ApiObjectSchema):
    path = fields.String()


class ErrorInfoSchema(ApiObjectSchema):
    msg = fields.String()
    help_msg = fields.String(allow_none=True)
    databand_error = fields.Bool()
    exc_type = fields.Function(lambda obj: str(obj.exc_type), allow_none=True)
    traceback = fields.String()
    nested = fields.String(allow_none=True)
    user_code_traceback = fields.String()
    show_exc_info = fields.Bool()

    @post_load
    def make_object(self, data, **kwargs):
        return ErrorInfo(**data)


class TaskRunAttemptSchema(ApiObjectSchema):
    task_run_uid = fields.UUID()
    task_run_attempt_uid = fields.UUID()
    state = EnumField(TaskRunState, allow_none=True)
    timestamp = fields.DateTime(allow_none=True)
    first_error = fields.Nested(ErrorInfoSchema, allow_none=True)
    latest_error = fields.Nested(ErrorInfoSchema, allow_none=True)
    attempt_number = fields.Number(allow_none=True)
    source = fields.Str(allow_none=True)
    start_date = fields.DateTime(allow_none=True)
    external_links_dict = fields.Dict(allow_none=True)


class ExternalUrlSchema(ApiObjectSchema):
    id = fields.UUID(attribute="uid")
    name = fields.Str()
    url = fields.Str()
