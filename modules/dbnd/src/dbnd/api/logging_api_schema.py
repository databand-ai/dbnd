from dbnd._core.tracking.schemas.base import _ApiCallSchema
from dbnd._vendor.marshmallow import fields


class LogMessageSchema(_ApiCallSchema):
    source = fields.String(allow_none=True)
    stack_trace = fields.String(allow_none=True)
    timestamp = fields.DateTime(allow_none=True)
