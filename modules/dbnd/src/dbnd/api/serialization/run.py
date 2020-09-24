from dbnd._core.constants import RunState
from dbnd._core.tracking.schemas.base import ApiObjectSchema
from dbnd._core.utils.dotdict import _as_dotted_dict
from dbnd._vendor.marshmallow import fields, post_load
from dbnd._vendor.marshmallow_enum import EnumField


class ScheduledRunInfoSchema(ApiObjectSchema):
    scheduled_job_uid = fields.UUID(allow_none=True)
    scheduled_date = fields.DateTime(allow_none=True)
    scheduled_job_dag_run_id = fields.String(allow_none=True)
    scheduled_job_name = fields.String(allow_none=True)

    @post_load
    def make_object(self, data, **kwargs):
        return _as_dotted_dict(**data)


class RootRunInfoSchema(ApiObjectSchema):
    root_run_uid = fields.UUID()
    root_task_run_uid = fields.UUID(allow_none=True)
    root_task_run_attempt_uid = fields.UUID(allow_none=True)
    root_run_url = fields.String(allow_none=True)

    @post_load
    def make_object(self, data, **kwargs):
        return _as_dotted_dict(**data)


class RunInfoSchema(ApiObjectSchema):
    root_run_uid = fields.UUID()
    run_uid = fields.UUID()

    job_name = fields.String()
    user = fields.String()

    name = fields.String()
    description = fields.String(allow_none=True)

    state = EnumField(RunState)
    start_time = fields.DateTime()
    end_time = fields.DateTime(allow_none=True)

    # deprecate
    dag_id = fields.String()
    cmd_name = fields.String(allow_none=True)

    execution_date = fields.DateTime()

    # move to task
    target_date = fields.Date(allow_none=True)
    version = fields.String(allow_none=True)

    driver_name = fields.String()
    is_archived = fields.Boolean()
    env_name = fields.String(allow_none=True)
    cloud_type = fields.String()
    trigger = fields.String()
    task_executor = fields.String(allow_none=True)

    root_run = fields.Nested(RootRunInfoSchema)
    scheduled_run = fields.Nested(ScheduledRunInfoSchema, allow_none=True)

    sends_heartbeat = fields.Boolean(default=False, allow_none=True)

    scheduled_job_name = fields.String(allow_none=True)
    scheduled_date = fields.DateTime(allow_none=True)

    @post_load
    def make_run_info(self, data, **kwargs):
        return _as_dotted_dict(**data)
