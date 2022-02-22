from dbnd._core.tracking.schemas.base import ApiStrictSchema
from dbnd._vendor.croniter import croniter
from dbnd._vendor.marshmallow import Schema, fields


class AlertEventSchema(Schema):
    name = fields.Str()
    description = fields.Str()
    run_uid = fields.Str()


class ScheduledJobSchemaV2(ApiStrictSchema):
    uid = fields.Str(attribute="DbndScheduledJob.uid", allow_none=True)
    name = fields.Str(attribute="DbndScheduledJob.name", required=True)
    cmd = fields.Str(attribute="DbndScheduledJob.cmd", required=True)
    schedule_interval = fields.Str(
        attribute="DbndScheduledJob.schedule_interval", required=True
    )
    start_date = fields.DateTime(
        allow_none=True, attribute="DbndScheduledJob.start_date", format="iso"
    )
    end_date = fields.DateTime(
        allow_none=True, attribute="DbndScheduledJob.end_date", format="iso"
    )
    readable_schedule_interval = fields.Str(
        attribute="DbndScheduledJob.readable_schedule_interval", allow_none=True
    )
    scheduled_interval_in_seconds = fields.Integer(
        attribute="DbndScheduledJob.scheduled_interval_in_seconds", allow_none=True
    )
    catchup = fields.Boolean(allow_none=True, attribute="DbndScheduledJob.catchup")
    depends_on_past = fields.Boolean(
        allow_none=True, attribute="DbndScheduledJob.depends_on_past"
    )
    retries = fields.Int(allow_none=True, attribute="DbndScheduledJob.retries")

    active = fields.Boolean(allow_none=True, attribute="DbndScheduledJob.active")
    create_user = fields.Str(allow_none=True, attribute="DbndScheduledJob.create_user")
    create_time = fields.DateTime(
        allow_none=True, attribute="DbndScheduledJob.create_time"
    )
    update_user = fields.Str(allow_none=True, attribute="DbndScheduledJob.update_user")
    update_time = fields.DateTime(
        allow_none=True, attribute="DbndScheduledJob.update_time"
    )
    from_file = fields.Boolean(allow_none=True, attribute="DbndScheduledJob.from_file")
    deleted_from_file = fields.Boolean(
        allow_none=True, attribute="DbndScheduledJob.deleted_from_file"
    )
    next_job_date = fields.DateTime(
        attribute="DbndScheduledJob.next_job_date", allow_none=True
    )
    alerts = fields.List(
        fields.Nested(AlertEventSchema),
        attribute="DbndScheduledJob.alerts",
        allow_none=True,
    )

    job_name = fields.Str(dump_only=True, attribute="DbndScheduledJob.job_name")
    job_id = fields.Str()
    last_run_uid = fields.UUID(dump_only=True)
    last_run_job = fields.Str(dump_only=True)
    last_job_date = fields.DateTime(dump_only=True)
    last_run_state = fields.Str(dump_only=True)
    latest_run_env = fields.Str(dump_only=True)
    root_task_run_uid = fields.Str(dump_only=True)
    is_airflow_synced = fields.Bool(dump_only=True)
    source_instance_name = fields.Str(allow_none=True)
    source_type = fields.Str(allow_none=True)
    # TODO_API: depredate
    airflow_instance_name = fields.Str(allow_none=True)
    list_order = fields.Integer(
        attribute="DbndScheduledJob.list_order", allow_none=True
    )
    validation_errors = fields.Str(
        allow_none=True, attribute="DbndScheduledJob.validation_errors"
    )
    extra_args = fields.Str(allow_none=True, attribute="DbndScheduledJob.extra_args")
    project_id = fields.Int(dump_only=True)
    project_name = fields.Str(dump_only=True)


SCHEDULE_INTERVAL_PRESETS = {
    "@once": None,
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly": "0 0 1 1 *",
}


def validate_cron(expression):
    if expression in SCHEDULE_INTERVAL_PRESETS:
        return None

    try:
        croniter.expand(expression)
    except Exception as e:
        return str(e)
    else:
        return None
