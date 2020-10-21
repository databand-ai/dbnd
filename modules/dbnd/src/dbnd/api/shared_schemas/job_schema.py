from dbnd._core.tracking.schemas.base import ApiObjectSchema
from dbnd._vendor.marshmallow import fields, validate


class JobSchemaV2(ApiObjectSchema):
    id = fields.Int()
    name = fields.Str()
    user = fields.Str()
    ui_hidden = fields.Boolean()
    is_airflow_synced = fields.Boolean()

    # computed
    run_states = fields.Dict()
    airflow_link = fields.Str()

    # joined
    latest_run_start_time = fields.DateTime()
    latest_run_state = fields.Str()
    latest_run_uid = fields.UUID()
    latest_run_root_task_run_uid = fields.UUID()
    latest_run_trigger = fields.Str()
    latest_run_env = fields.Str()

    scheduled_job_count = fields.Number()


class JobsSetArchiveSchema(ApiObjectSchema):
    ids = fields.List(fields.Integer(), required=True, validate=validate.Length(min=1))
    is_archived = fields.Boolean(required=True)


jobs_set_archive_schema = JobsSetArchiveSchema()
