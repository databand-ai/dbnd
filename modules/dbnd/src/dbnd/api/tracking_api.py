import typing

from datetime import datetime
from typing import List, Optional, Tuple
from uuid import UUID

import attr

from dbnd._core.constants import (
    DbndDatasetOperationType,
    DbndTargetOperationStatus,
    DbndTargetOperationType,
    RunState,
    TaskRunState,
    UpdateSource,
)
from dbnd._core.tracking.airflow_dag_inplace_tracking import AirflowTaskContext
from dbnd._core.tracking.schemas.base import ApiStrictSchema
from dbnd._core.tracking.schemas.column_stats import ColumnStatsArgs, ColumnStatsSchema
from dbnd._core.tracking.schemas.metrics import Metric
from dbnd._core.tracking.schemas.tracking_info_run import RunInfo, ScheduledRunInfo
from dbnd._core.utils import json_utils
from dbnd._core.utils.dotdict import _as_dotted_dict
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor._marshmallow.decorators import pre_load
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
from targets.data_schema import DataSchemaArgs, StructuredDataSchema


if typing.TYPE_CHECKING:
    from dbnd._core.tracking.schemas.tracking_info_objects import (
        ErrorInfo,
        TargetInfo,
        TaskDefinitionInfo,
        TaskRunEnvInfo,
        TaskRunInfo,
    )

AIRFLOW_SOURCE_TYPE = "airflow"


@attr.s
class TrackingSource(object):
    name = attr.ib()  # type: str
    url = attr.ib()  # type: str
    env = attr.ib()  # type: str

    source_type = attr.ib(default=AIRFLOW_SOURCE_TYPE)  # type: str
    source_instance_uid = attr.ib(default=None)  # type: Optional[UUID]


class TrackingSourceSchema(ApiStrictSchema):
    name = fields.String()
    url = fields.String()
    env = fields.String()

    source_type = fields.String()
    source_instance_uid = fields.UUID()

    @post_load
    def make_run_info(self, data, **kwargs):
        return TrackingSource(**data)


class AirflowTaskContextSchema(ApiStrictSchema):
    dag_id = fields.String()
    execution_date = fields.String()
    task_id = fields.String()
    try_number = fields.Integer(allow_none=True)
    airflow_instance_uid = fields.UUID()

    @post_load
    def make_run_info(self, data, **kwargs):
        return AirflowTaskContext(**data)


class TaskRunsInfoSchema(ApiStrictSchema):
    run_uid = fields.UUID()
    root_run_uid = fields.UUID()
    task_run_env_uid = fields.UUID()

    task_runs = fields.Nested(
        TaskRunInfoSchema, many=True, exclude=("task_signature_source",)
    )

    parent_child_map = fields.List(fields.List(fields.UUID()))
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
    af_with_monitor = attr.ib(default=True)  # type: bool
    af_context = attr.ib(default=None)  # type: Optional[AirflowTaskContext]
    tracking_source = attr.ib(default=None)  # type: Optional[TrackingSource]

    def asdict(self):
        return attr.asdict(self, recurse=False)

    @property
    def source_instance_uid(self) -> Optional[UUID]:
        """
        Currently (2022/01) this data is sent only when we have some known
        orchestration, i.e airflow or azkaban. If tracking data is sent from
        unknown/unsupported orchestrator (including also standalone script execution),
        or from dbnd orchestration - there will be no source_instance_uid.

        Will return source_instance_uid from any of specific contexts -
        either from airflow_context or from more generic tracking_source

        Will return None if no tracking_sources defined.
        """
        if self.af_context and self.af_context.airflow_instance_uid:
            return self.af_context.airflow_instance_uid

        if self.tracking_source and self.tracking_source.source_instance_uid:
            return self.tracking_source.source_instance_uid
        return None


class InitRunArgsSchema(ApiStrictSchema):
    run_uid = fields.UUID()
    root_run_uid = fields.UUID()

    driver_task_uid = fields.UUID(allow_none=True)

    task_run_env = fields.Nested(TaskRunEnvInfoSchema)
    task_runs_info = fields.Nested(TaskRunsInfoSchema)

    new_run_info = fields.Nested(RunInfoSchema, allow_none=True)
    scheduled_run_info = fields.Nested(ScheduledRunInfoSchema, allow_none=True)
    update_existing = fields.Boolean()
    af_with_monitor = fields.Boolean()
    source = fields.Str(allow_none=True)
    af_context = fields.Nested(AirflowTaskContextSchema, allow_none=True)
    tracking_source = fields.Nested(TrackingSourceSchema, allow_none=True)

    @post_load
    def make_init_run_args(self, data, **kwargs):
        return InitRunArgs(**data)


@attr.s
class TaskRunAttemptUpdateArgs(object):
    task_run_uid = attr.ib(repr=False)  # type: UUID
    task_run_attempt_uid = attr.ib(repr=False)  # type: UUID
    timestamp = attr.ib()  # type:  datetime
    state = attr.ib()  # type: TaskRunState
    error = attr.ib(default=None)  # type: Optional[ErrorInfo]
    attempt_number = attr.ib(default=None)  # type: int
    source = attr.ib(default=UpdateSource.dbnd)  # type: UpdateSource
    start_date = attr.ib(default=None)  # type: datetime
    external_links_dict = attr.ib(default=None)


class TaskRunAttemptUpdateArgsSchema(ApiStrictSchema):
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


class InitRunSchema(ApiStrictSchema):
    init_args = fields.Nested(InitRunArgsSchema)
    version = fields.String(allow_none=True)


init_run_schema = InitRunSchema()


class AddTaskRunsSchema(ApiStrictSchema):
    task_runs_info = fields.Nested(TaskRunsInfoSchema)
    source = EnumField(UpdateSource, allow_none=True)


add_task_runs_schema = AddTaskRunsSchema()


class SetDatabandRunStateSchema(ApiStrictSchema):
    run_uid = fields.UUID(required=True)
    state = EnumField(RunState)
    timestamp = fields.DateTime(required=False, default=None, missing=None)


set_run_state_schema = SetDatabandRunStateSchema()


class SetTaskRunReusedSchema(ApiStrictSchema):
    task_run_uid = fields.UUID(required=True)
    task_outputs_signature = fields.String(required=True)


set_task_run_reused_schema = SetTaskRunReusedSchema()


class UpdateTaskRunAttemptsSchema(ApiStrictSchema):
    task_run_attempt_updates = fields.Nested(TaskRunAttemptUpdateArgsSchema, many=True)


update_task_run_attempts_schema = UpdateTaskRunAttemptsSchema()


class SetUnfinishedTasksStateShcmea(ApiStrictSchema):
    run_uid = fields.UUID()
    state = EnumField(TaskRunState)
    timestamp = fields.DateTime()


set_unfinished_tasks_state_schema = SetUnfinishedTasksStateShcmea()


class SaveTaskRunLogSchema(ApiStrictSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    log_body = fields.String(allow_none=True)
    local_log_path = fields.String(allow_none=True)


save_task_run_log_schema = SaveTaskRunLogSchema()


class SaveExternalLinksSchema(ApiStrictSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    external_links_dict = fields.Dict(
        name=fields.Str(), url=fields.Str(), required=True
    )


save_external_links_schema = SaveExternalLinksSchema()


class LogMetricSchema(ApiStrictSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    target_meta_uid = fields.UUID(allow_none=True)
    metric = fields.Nested(MetricSchema, allow_none=True)
    source = fields.String(allow_none=True)


log_metric_schema = LogMetricSchema()


class LogMetricsSchema(ApiStrictSchema):
    metrics_info = fields.Nested(LogMetricSchema, many=True)

    @post_load
    def make_object(self, data, **kwargs):
        data["metrics_info"] = [LogMetricArgs(**m) for m in data["metrics_info"]]


log_metrics_schema = LogMetricsSchema()


class DbtMetadataSchema(ApiStrictSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    dbt_run_metadata = fields.Dict(required=True)


log_dbt_metadata_schema = DbtMetadataSchema()


class LogArtifactSchema(ApiStrictSchema):
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

    def copy(self, key, value):
        return LogMetricArgs(
            task_run_attempt_uid=self.task_run_attempt_uid,
            source=self.source,
            target_meta_uid=self.target_meta_uid,
            metric=Metric(
                key=key,
                value=value,
                timestamp=self.metric.timestamp,
                source=self.metric.source,
            ),
        )


@attr.s
class LogDatasetArgs(object):
    run_uid = attr.ib()  # type: UUID
    task_run_uid = attr.ib()  # type: UUID
    task_run_name = attr.ib()  # type: str
    task_run_attempt_uid = attr.ib()  # type: UUID

    operation_path = attr.ib()  # type: Optional[str]

    operation_type = attr.ib()  # type: DbndDatasetOperationType
    operation_status = attr.ib()  # type: DbndTargetOperationStatus

    value_preview = attr.ib()  # type: Optional[str]
    data_dimensions = attr.ib()  # type: Optional[Tuple[Optional[int], Optional[int]]]
    data_schema = attr.ib()  # type: Optional[DataSchemaArgs]

    query = attr.ib(default=None)  # type: Optional[str]
    dataset_uri = attr.ib(default=None)  # type: Optional[str]
    columns_stats = attr.ib(default=attr.Factory(list))  # type: List[ColumnStatsArgs]

    operation_error = attr.ib(
        default=None
    )  # type: str # default=None :-> for compatibility with SDK < 51.0.0
    operation_source = attr.ib(default=None)  # type: Optional[str]
    timestamp = attr.ib(default=attr.Factory(utcnow))  # type: datetime
    with_partition = attr.ib(default=None)  # type: Optional[bool]

    def asdict(self):
        return attr.asdict(self, recurse=False)

    @property
    def records(self):
        return self.data_dimensions[0] if self.data_dimensions else None

    @property
    def columns(self):
        return self.data_dimensions[1] if self.data_dimensions else None

    @property
    def operation_status_value(self):
        return "SUCCESS" if self.operation_status.value == "OK" else "FAILED"


class LogDatasetSchema(ApiStrictSchema):
    run_uid = fields.UUID(required=True)
    task_run_uid = fields.UUID(required=True)
    task_run_name = fields.String(required=True)
    task_run_attempt_uid = fields.UUID(required=True)

    operation_path = fields.String(allow_none=True)
    dataset_uri = fields.String(allow_none=True, default=None)

    operation_type = EnumField(DbndDatasetOperationType)
    operation_status = EnumField(DbndTargetOperationStatus)
    operation_error = fields.String(allow_none=True)
    operation_source = fields.String(allow_none=True)
    timestamp = fields.DateTime(required=False, allow_none=True)

    value_preview = fields.String(allow_none=True)
    data_dimensions = fields.List(fields.Integer(allow_none=True), allow_none=True)
    data_schema = fields.Nested(StructuredDataSchema, allow_none=True)
    query = fields.String(allow_none=True)
    columns_stats = fields.Nested(ColumnStatsSchema, many=True, required=False)

    with_partition = fields.Boolean(required=False, allow_none=True)

    @pre_load
    def pre_load(self, data: dict) -> dict:
        data_schema = data.get("data_schema")
        if data_schema:
            # Backwared compatible with older SDK versions < v60.0.0
            if isinstance(data_schema, str):
                data["data_schema"] = json_utils.loads(data_schema)
        return data

    @post_load
    def make_object(self, data):
        return LogDatasetArgs(**data).asdict()


class LogDatasetsSchema(ApiStrictSchema):
    datasets_info = fields.Nested(LogDatasetSchema, many=True)


log_datasets_schema = LogDatasetsSchema()


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
    data_dimensions = attr.ib()  # type: Optional[Tuple[Optional[int], Optional[int]]]
    data_schema = attr.ib()  # type: str
    data_hash = attr.ib()  # type: str

    param_name = attr.ib(default=None)  # type: Optional[str]
    task_def_uid = attr.ib(default=None)  # type: Optional[UUID]

    def asdict(self):
        return attr.asdict(self, recurse=False)


class LogTargetSchema(ApiStrictSchema):
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


class LogTargetsSchema(ApiStrictSchema):
    targets_info = fields.Nested(LogTargetSchema, many=True)


log_targets_schema = LogTargetsSchema()


class HeartbeatSchema(ApiStrictSchema):
    run_uid = fields.UUID()


heartbeat_schema = HeartbeatSchema()


class AirflowTaskInfoSchema(ApiStrictSchema):
    execution_date = fields.DateTime()
    last_sync = fields.DateTime(allow_none=True)
    dag_id = fields.String()
    task_id = fields.String()
    task_run_attempt_uid = fields.UUID()
    retry_number = fields.Integer(required=False, allow_none=True)

    @post_load
    def make_object(self, data, **kwargs):
        return AirflowTaskInfo(**data)


class AirflowTaskInfosSchema(ApiStrictSchema):
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
    start_date = attr.ib()  # type: datetime
    create_user = attr.ib()  # type: str
    create_time = attr.ib()  # type: datetime
    end_date = attr.ib(default=None)  # type: Optional[datetime]
    schedule_interval = attr.ib(default=None)  # type: Optional[str]
    catchup = attr.ib(default=None)  # type: Optional[bool]
    depends_on_past = attr.ib(default=None)  # type: Optional[bool]
    retries = attr.ib(default=None)  # type: Optional[int]
    active = attr.ib(default=None)  # type: Optional[bool]
    update_user = attr.ib(default=None)  # type: Optional[str]
    update_time = attr.ib(default=None)  # type: Optional[datetime]
    from_file = attr.ib(default=False)  # type: bool
    deleted_from_file = attr.ib(default=False)  # type: bool
    list_order = attr.ib(default=None)  # type: Optional[List[int]]
    job_name = attr.ib(default=None)  # type: Optional[str]


class ScheduledJobInfoSchema(ApiStrictSchema):
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


class ScheduledJobArgsSchema(ApiStrictSchema):
    scheduled_job_args = fields.Nested(ScheduledJobInfoSchema)
    update_existing = fields.Boolean(default=False)


scheduled_job_args_schema = ScheduledJobArgsSchema()


@attr.s
class AirflowTaskInfo(object):
    execution_date = attr.ib()  # type: datetime
    dag_id = attr.ib()  # type: str
    task_id = attr.ib()  # type: str
    task_run_attempt_uid = attr.ib()  # type: UUID
    last_sync = attr.ib(default=None)  # type: Optional[datetime]
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
