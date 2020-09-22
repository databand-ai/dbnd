from dbnd._core.tracking.schemas.base import _ApiCallSchema
from dbnd._vendor.marshmallow import fields


class LogTargetMetricsSchema(_ApiCallSchema):
    task_run_uid = fields.UUID(required=True)
    task_run_attempt_uid = fields.UUID(required=True)

    target_path = fields.String()

    value_preview = fields.String(allow_none=True)
    data_dimensions = fields.List(fields.Integer(), allow_none=True)
    data_schema = fields.String(allow_none=True)


log_target_metrics_schema = LogTargetMetricsSchema()
