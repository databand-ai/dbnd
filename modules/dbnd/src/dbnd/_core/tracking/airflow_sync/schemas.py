import attr

from dbnd._core.utils.timezone import utcnow


@attr.s
class ExportData(object):
    init_args = attr.ib(factory=list)
    task_run_attempt_updates = attr.ib(factory=list)
    scheduled_job_infos = attr.ib(factory=list)
    updated_runs = attr.ib(factory=list)
    logs = attr.ib(factory=list)  # type: List[SaveTaskRunLog]
    run_states = attr.ib(factory=list)  # type: List[SetRunStateArgs]
    timestamp = attr.ib(factory=utcnow)


# class ExportDataSchema(ApiObjectSchema):
#     init_args = fields.Nested(InitRunArgsSchema, many=True)
#     task_run_attempt_updates = fields.Nested(TaskRunAttemptUpdateArgsSchema, many=True)
#     scheduled_job_infos = fields.Nested(ScheduledJobInfoSchema, many=True)
#     logs = fields.Nested(SaveTaskRunLogSchema, many=True)
#     run_states = fields.Nested(SetDatabandRunStateSchema, many=True)
#     updated_runs = fields.List(fields.List(fields.String()))
#     timestamp = fields.DateTime()
#
#     short_response_fields = ("updated_runs", "timestamp")
#
#     @post_load
#     def make_init_run_args(self, data, **kwargs):
#         # we need this while SaveTaskRunLog is not created automatically from SaveTaskRunLogSchema
#         if data.get("logs"):
#             data["logs"] = [SaveTaskRunLog(**i) for i in data["logs"]]
#         # we need those while SetRunStateArgs is not created automatically from SetDatabandRunStateSchema
#         if data.get("run_states"):
#             data["run_states"] = [SetRunStateArgs(**i) for i in data["run_states"]]
#         return ExportData(**data)
#
#
@attr.s
class SaveTaskRunLog(object):
    task_run_attempt_uid = attr.ib()
    log_body = attr.ib()


@attr.s
class SetRunStateArgs(object):
    run_uid = attr.ib()
    state = attr.ib()
    timestamp = attr.ib()
