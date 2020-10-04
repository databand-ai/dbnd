from dbnd._core.tracking.schemas.base import ApiObjectSchema
from dbnd._vendor.marshmallow import fields


class AlertDefsSchema(ApiObjectSchema):
    uid = fields.Str()
    original_uid = fields.Str()
    custom_name = fields.Str()
    custom_description = fields.Str()
    summary = fields.Str()

    severity = fields.Str()
    type = fields.Str()
    task_name = fields.Str()
    user_metric = fields.Str()
    operator = fields.Str()
    is_str_value = fields.Bool()
    value = fields.Str()
    advanced_json = fields.Str()

    created_at = fields.DateTime()
    scheduled_job_uid = fields.Str()
    scheduled_job_name = fields.Str(attribute="scheduled_job.name")
    job_name = fields.Str()
