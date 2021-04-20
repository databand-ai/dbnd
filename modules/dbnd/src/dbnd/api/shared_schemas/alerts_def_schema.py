from dbnd._core.tracking.schemas.base import ApiObjectSchema
from dbnd._vendor.marshmallow import fields


class MLAlert(ApiObjectSchema):
    sensitivity = fields.Float()
    look_back = fields.Integer()


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

    ml_alert = fields.Nested(MLAlert, allow_none=True)

    created_at = fields.DateTime()
    scheduled_job_uid = fields.Str()
    scheduled_job_name = fields.Str(attribute="scheduled_job.name")
    job_name = fields.Str()
    job_id = fields.Int()
    source_instance_name = fields.Str(attribute="job.tracking_source.name")
    env = fields.Str(attribute="job.tracking_source.env")
    # TODO_CORE: API: Deprecate airflow_server_info
    airflow_instance_name = fields.Str(attribute="job.tracking_source.name")

    project_id = fields.Int(attribute="job.project_id")
    project_name = fields.Str(attribute="job.project.name")
