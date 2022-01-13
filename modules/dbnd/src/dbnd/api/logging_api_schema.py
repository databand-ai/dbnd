from dbnd._core.tracking.schemas.base import ApiStrictSchema
from dbnd._vendor.marshmallow import fields


class LogMessageSchema(ApiStrictSchema):
    source = fields.String(allow_none=True)
    stack_trace = fields.String(allow_none=True)
    timestamp = fields.DateTime(allow_none=True)
    dbnd_version = fields.String(allow_none=True, missing=None)
