import datetime
import typing

from uuid import UUID

import attr

from dbnd._core.constants import (
    DbndTargetOperationStatus,
    DbndTargetOperationType,
    RunState,
    TaskRunState,
    UpdateSource,
)
from dbnd._core.inplace_run.airflow_dag_inplace_tracking import AirflowTaskContext
from dbnd._core.tracking.schemas.base import ApiObjectSchema, _ApiCallSchema
from dbnd._core.tracking.schemas.metrics import Metric
from dbnd._core.tracking.schemas.tracking_info_run import RunInfo, ScheduledRunInfo
from dbnd._core.utils.dotdict import _as_dotted_dict
from dbnd._vendor.marshmallow import fields, post_load
from dbnd._vendor.marshmallow_enum import EnumField
from dbnd.api.serialization.common import (
    ErrorInfoSchema,
    MetricSchema,
    TargetInfoSchema,
)
from dbnd.api.serialization.run import RunInfoSchema, ScheduledRunInfoSchema
from dbnd.api.serialization.task import TaskDefinitionInfoSchema, TaskRunInfoSchema
from dbnd.api.serialization.task_run_env import TaskRunEnvInfoSchema


if typing.TYPE_CHECKING:
    from typing import Optional, List, Dict, Tuple, Sequence
    from dbnd._core.tracking.schemas.tracking_info_objects import (
        ErrorInfo,
        TaskRunEnvInfo,
        TargetInfo,
        TaskRunInfo,
        TaskDefinitionInfo,
    )


class AirflowTaskContextSchema(ApiObjectSchema):
    dag_id = fields.String()
    execution_date = fields.String()
    task_id = fields.String()
    try_number = fields.Integer(allow_none=True)
    airflow_instance_uid = fields.UUID()

    @post_load
    def make_run_info(self, data, **kwargs):
        return AirflowTaskContext(**data)


class TaskRunsInfoSchema(ApiObjectSchema):
    run_uid = fields.UUID()
    root_run_uid = fields.UUID()
    task_run_env_uid = fields.UUID()

    parent_child_map = fields.List(fields.List(fields.UUID()))
    task_runs = fields.Nested(
        TaskRunInfoSchema, many=True, exclude=("task_signature_source",)
    )
    upstreams_map = fields.List(fields.List(fields.UUID()))
    dynamic_task_run_update = fields.Boolean()

    targets = fields.Nested(TargetInfoSchema, many=True)
    task_definitions = fields.Nested(TaskDefinitionInfoSchema, many=True)
    af_context = fields.Nested(AirflowTaskContextSchema, allow_none=True)

    parent_task_run_uid = fields.UUID(allow_none=True)
    parent_task_run_attempt_uid = fields.UUID(allow_none=True)

    @post_load
    def make_run_info(self, data, **kwargs):
        return _as_dotted_dict(**data)


@attr.s
class InitRunArgs(object):
    root_run_uid = attr.ib()  # type: UUID
    run_uid = attr.ib()  # type: UUID
    task_runs_info = attr.ib()  # type: TaskRunsInfo
    driver_task_uid = attr.ib(default=None)  # type: UUID

    task_run_env = attr.ib(default=None)  # type: TaskRunEnvInfo

    new_run_info = attr.ib(
        default=None
    )  # type: Optional[RunInfo]  # we create it only for the new runs
    scheduled_run_info = attr.ib(default=None)  # type: Optional[ScheduledRunInfo]
    update_existing = attr.ib(default=False)  # type: bool
    source = attr.ib(default=UpdateSource.dbnd)  # type: UpdateSource
    af_context = attr.ib(default=None)  # type: Optional[AirflowTaskContext]

    def asdict(self):
        return attr.asdict(self, recurse=False)


class InitRunArgsSchema(ApiObjectSchema):
    run_uid = fields.UUID()
    root_run_uid = fields.UUID()

    driver_task_uid = fields.UUID(allow_none=True)

    task_run_env = fields.Nested(TaskRunEnvInfoSchema)
    task_runs_info = fields.Nested(TaskRunsInfoSchema)

    new_run_info = fields.Nested(RunInfoSchema, allow_none=True)
    scheduled_run_info = fields.Nested(ScheduledRunInfoSchema, allow_none=True)
    update_existing = fields.Boolean()
    source = fields.Str(allow_none=True)
    af_context = fields.Nested(AirflowTaskContextSchema, allow_none=True)

    @post_load
    def make_init_run_args(self, data, **kwargs):
        return InitRunArgs(**data)


@attr.s
class TaskRunAttemptUpdateArgs(object):
    task_run_uid = attr.ib()  # type: UUID
    task_run_attempt_uid = attr.ib()  # type: UUID
    timestamp = attr.ib()  # type:  datetime.datetime
    state = attr.ib()  # type: TaskRunState
    error = attr.ib(default=None)  # type: Optional[ErrorInfo]
    attempt_number = attr.ib(default=None)  # type: int
    source = attr.ib(default=UpdateSource.dbnd)  # type: UpdateSource
    start_date = attr.ib(default=None)  # type: datetime.datetime
    external_links_dict = attr.ib(default=None)


class TaskRunAttemptUpdateArgsSchema(ApiObjectSchema):
    task_run_uid = fields.UUID()
    task_run_attempt_uid = fields.UUID()
    state = EnumField(TaskRunState, allow_none=True)
    timestamp = fields.DateTime(allow_none=True)
    error = fields.Nested(ErrorInfoSchema, allow_none=True)
    attempt_number = fields.Number(allow_none=True)
    source = fields.Str(allow_none=True)
    start_date = fields.DateTime(allow_none=True)
    external_links_dict = fields.Dict(allow_none=True)

    @post_load
    def make_object(self, data, **kwargs):
        return TaskRunAttemptUpdateArgs(**data)


class InitRunSchema(_ApiCallSchema):
    init_args = fields.Nested(InitRunArgsSchema)


init_run_schema = InitRunSchema()


class AddTaskRunsSchema(_ApiCallSchema):
    task_runs_info = fields.Nested(TaskRunsInfoSchema)
    source = EnumField(UpdateSource, allow_none=True)


add_task_runs_schema = AddTaskRunsSchema()


class SetDatabandRunStateSchema(_ApiCallSchema):
    run_uid = fields.UUID(required=True)
    state = EnumField(RunState)
    timestamp = fields.DateTime(required=False, default=None, missing=None)


set_run_state_schema = SetDatabandRunStateSchema()


class SetTaskRunReusedSchema(_ApiCallSchema):
    task_run_uid = fields.UUID(required=True)
    task_outputs_signature = fields.String(required=True)


set_task_run_reused_schema = SetTaskRunReusedSchema()


class UpdateTaskRunAttemptsSchema(_ApiCallSchema):
    task_run_attempt_updates = fields.Nested(TaskRunAttemptUpdateArgsSchema, many=True)


update_task_run_attempts_schema = UpdateTaskRunAttemptsSchema()


class SetUnfinishedTasksStateShcmea(_ApiCallSchema):
    run_uid = fields.UUID()
    state = EnumField(TaskRunState)
    timestamp = fields.DateTime()


set_unfinished_tasks_state_schema = SetUnfinishedTasksStateShcmea()


class SaveTaskRunLogSchema(_ApiCallSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    log_body = fields.String(allow_none=True)
    local_log_path = fields.String(allow_none=True)


save_task_run_log_schema = SaveTaskRunLogSchema()


class SaveExternalLinksSchema(_ApiCallSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    external_links_dict = fields.Dict(
        name=fields.Str(), url=fields.Str(), required=True
    )


save_external_links_schema = SaveExternalLinksSchema()


class LogMetricSchema(_ApiCallSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    target_meta_uid = fields.UUID(allow_none=True)
    metric = fields.Nested(MetricSchema, allow_none=True)
    source = fields.String(allow_none=True)


log_metric_schema = LogMetricSchema()


class LogMetricsSchema(_ApiCallSchema):
    metrics_info = fields.Nested(LogMetricSchema, many=True)

    @post_load
    def make_object(self, data, **kwargs):
        data["metrics_info"] = [LogMetricArgs(**m) for m in data["metrics_info"]]


log_metrics_schema = LogMetricsSchema()


class LogArtifactSchema(_ApiCallSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    name = fields.String()
    path = fields.String()


log_artifact_schema = LogArtifactSchema()


@attr.s
class LogMetricArgs(object):
    task_run_attempt_uid = attr.ib()  # type: UUID
    metric = attr.ib()  # type: Metric
    source = attr.ib(default=None)  # type: Optional[str]
    target_meta_uid = attr.ib(default=None)  # type: Optional[UUID]


@attr.s
class LogTargetArgs(object):
    run_uid = attr.ib()  # type: UUID
    task_run_uid = attr.ib()  # type: UUID
    task_run_name = attr.ib()  # type: str
    task_run_attempt_uid = attr.ib()  # type: UUID
    target_path = attr.ib()  # type: str
    operation_type = attr.ib()  # type: DbndTargetOperationType
    operation_status = attr.ib()  # type: DbndTargetOperationStatus

    value_preview = attr.ib()  # type: str
    data_dimensions = attr.ib()  # type: Sequence[int]
    data_schema = attr.ib()  # type: str
    data_hash = attr.ib()  # type: str

    param_name = attr.ib(default=None)  # type: Optional[str]
    task_def_uid = attr.ib(default=None)  # type: Optional[UUID]

    def asdict(self):
        return attr.asdict(self, recurse=False)


@attr.s
class LogDataframeHistogramsArgs(object):
    run_uid = attr.ib()  # type: UUID
    task_run_uid = attr.ib()  # type: UUID
    task_run_name = attr.ib()  # type: str
    task_run_attempt_uid = attr.ib()  # type: UUID
    target_path = attr.ib()  # type: str
    operation_type = attr.ib()  # type: DbndTargetOperationType
    operation_status = attr.ib()  # type: DbndTargetOperationStatus

    value_preview = attr.ib()  # type: str
    data_dimensions = attr.ib()  # type: Sequence[int]
    data_schema = attr.ib()  # type: str
    data_hash = attr.ib()  # type: str

    descriptive_stats = attr.ib()  # type: Dict[str, Dict]
    histograms = attr.ib()  # type: Dict[str, Tuple]
    timestamp = attr.ib()  # type: datetime.datetime

    param_name = attr.ib(default=None)  # type: Optional[str]
    task_def_uid = attr.ib(default=None)  # type: Optional[UUID]


class LogTargetSchema(_ApiCallSchema):
    run_uid = fields.UUID(required=True)
    task_run_uid = fields.UUID(required=True)
    task_run_name = fields.String()
    task_run_attempt_uid = fields.UUID(required=True)

    target_path = fields.String()
    param_name = fields.String(allow_none=True)
    task_def_uid = fields.UUID(allow_none=True)
    operation_type = EnumField(DbndTargetOperationType)
    operation_status = EnumField(DbndTargetOperationStatus)

    value_preview = fields.String(allow_none=True)
    data_dimensions = fields.List(fields.Integer(), allow_none=True)
    data_schema = fields.String(allow_none=True)
    data_hash = fields.String(allow_none=True)


class LogTargetsSchema(_ApiCallSchema):
    targets_info = fields.Nested(LogTargetSchema, many=True)


log_targets_schema = LogTargetsSchema()


class HeartbeatSchema(_ApiCallSchema):
    run_uid = fields.UUID()


heartbeat_schema = HeartbeatSchema()


class AirflowTaskInfoSchema(_ApiCallSchema):
    execution_date = fields.DateTime()
    last_sync = fields.DateTime(allow_none=True)
    dag_id = fields.String()
    task_id = fields.String()
    task_run_attempt_uid = fields.UUID()
    retry_number = fields.Integer(required=False, allow_none=True)

    @post_load
    def make_object(self, data, **kwargs):
        return AirflowTaskInfo(**data)


class AirflowTaskInfosSchema(_ApiCallSchema):
    airflow_task_infos = fields.Nested(AirflowTaskInfoSchema, many=True)
    source = fields.String()
    base_url = fields.String()


airflow_task_infos_schema = AirflowTaskInfosSchema()


@attr.s
class TaskRunsInfo(object):
    run_uid = attr.ib()  # type: UUID
    root_run_uid = attr.ib()  # type: UUID

    task_run_env_uid = attr.ib()  # type: UUID

    task_runs = attr.ib(default=None)  # type: List[TaskRunInfo]
    task_definitions = attr.ib(default=None)  # type: List[TaskDefinitionInfo]
    targets = attr.ib(default=None)  # type: List[TargetInfo]

    parent_child_map = attr.ib(default=None)
    upstreams_map = attr.ib(default=None)
    dynamic_task_run_update = attr.ib(default=False)
    af_context = attr.ib(default=None)  # type: Optional[AirflowTaskContext]

    parent_task_run_uid = attr.ib(default=None)  # type: Optional[UUID]
    parent_task_run_attempt_uid = attr.ib(default=None)  # type: Optional[UUID]


@attr.s
class ScheduledJobInfo(object):
    uid = attr.ib()  # type: UUID
    name = attr.ib()  # type: str
    cmd = attr.ib()  # type: str
    start_date = attr.ib()  # type: datetime.datetime
    create_user = attr.ib()  # type: str
    create_time = attr.ib()  # type: datetime.datetime
    end_date = attr.ib(default=None)  # type: Optional[datetime.datetime]
    schedule_interval = attr.ib(default=None)  # type: Optional[str]
    catchup = attr.ib(default=None)  # type: Optional[bool]
    depends_on_past = attr.ib(default=None)  # type: Optional[bool]
    retries = attr.ib(default=None)  # type: Optional[int]
    active = attr.ib(default=None)  # type: Optional[bool]
    update_user = attr.ib(default=None)  # type: Optional[str]
    update_time = attr.ib(default=None)  # type: Optional[datetime.datetime]
    from_file = attr.ib(default=False)  # type: bool
    deleted_from_file = attr.ib(default=False)  # type: bool
    list_order = attr.ib(default=None)  # type: Optional[List[int]]
    job_name = attr.ib(default=None)  # type: Optional[str]


class ScheduledJobInfoSchema(ApiObjectSchema):
    uid = fields.UUID()
    name = fields.String()
    cmd = fields.String()
    start_date = fields.DateTime()
    create_user = fields.String()
    create_time = fields.DateTime()
    end_date = fields.DateTime(allow_none=True)
    schedule_interval = fields.String(allow_none=True)
    catchup = fields.Boolean(allow_none=True)
    depends_on_past = fields.Boolean(allow_none=True)
    retries = fields.Integer(allow_none=True)
    active = fields.Boolean(allow_none=True)
    update_user = fields.String(allow_none=True)
    update_time = fields.DateTime(allow_none=True)
    from_file = fields.Boolean(allow_none=True)
    deleted_from_file = fields.Boolean(allow_none=True)
    list_order = fields.Integer(allow_none=True)
    job_name = fields.String(allow_none=True)

    @post_load
    def make_object(self, data, **kwargs):
        return ScheduledJobInfo(**data)


class ScheduledJobArgsSchema(_ApiCallSchema):
    scheduled_job_args = fields.Nested(ScheduledJobInfoSchema)
    update_existing = fields.Boolean(default=False)


scheduled_job_args_schema = ScheduledJobArgsSchema()


@attr.s
class AirflowTaskInfo(object):
    execution_date = attr.ib()  # type: datetime.datetime
    dag_id = attr.ib()  # type: str
    task_id = attr.ib()  # type: str
    task_run_attempt_uid = attr.ib()  # type: UUID
    last_sync = attr.ib(default=None)  # type: Optional[datetime.datetime]
    retry_number = attr.ib(default=None)  # type: Optional[int]

    def asdict(self):
        return dict(
            execution_date=self.execution_date,
            last_sync=self.last_sync,
            dag_id=self.dag_id,
            task_id=self.task_id,
            task_run_attempt_uid=self.task_run_attempt_uid,
            retry_number=self.retry_number,
        )


class AirflowMonitorDataSchema(ApiObjectSchema):
    airflow_export_data = fields.Str()
    airflow_base_url = fields.Str()
    last_sync_time = fields.DateTime()


airflow_monitor_data_schema = AirflowMonitorDataSchema()
