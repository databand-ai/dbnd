from dbnd._core.tracking.tracking_info_objects import (
    ErrorInfo,
    TargetInfo,
    TaskRunEnvInfo,
)
from dbnd._vendor.marshmallow import fields, post_load
from dbnd.api.api_utils import ApiObjectSchema


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
    value = fields.String(allow_none=True)
    value_int = fields.Integer(allow_none=True)
    value_float = fields.Float(allow_none=True)
    timestamp = fields.DateTime()

    @post_load
    def make_object(self, data, **kwargs):
        from dbnd._core.tracking.metrics import Metric

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


class ExternalUrlSchema(ApiObjectSchema):
    id = fields.UUID(attribute="uid")
    name = fields.Str()
    url = fields.Str()
