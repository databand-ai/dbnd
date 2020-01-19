import datetime
import logging
import typing

from abc import ABCMeta
from typing import Optional
from uuid import UUID

import six

from marshmallow import fields, post_load

import attr

from dbnd._core.constants import RunState, TaskRunState
from dbnd._core.tracking.tracking_info_objects import TaskRunEnvInfo
from dbnd._core.tracking.tracking_info_run import RunInfo, ScheduledRunInfo
from dbnd._vendor.marshmallow_enum import EnumField
from dbnd.api.api_utils import (
    ApiClient,
    ApiObjectSchema,
    _ApiCallSchema,
    _as_dotted_dict,
)
from dbnd.api.serialization.common import (
    ErrorInfoSchema,
    MetricSchema,
    TargetInfoSchema,
)
from dbnd.api.serialization.run import RunInfoSchema
from dbnd.api.serialization.task import TaskDefinitionInfoSchema, TaskRunInfoSchema
from dbnd.api.serialization.task_run_env import TaskRunEnvInfoSchema


if typing.TYPE_CHECKING:
    from dbnd._core.tracking.tracking_info_objects import ErrorInfo


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

    @post_load
    def make_run_info(self, data, **kwargs):
        return _as_dotted_dict(**data)


@attr.s
class InitRunArgs(object):
    root_run_uid = attr.ib()  # type: UUID
    run_uid = attr.ib()  # type: UUID
    task_runs_info = attr.ib()  # type: TaskRunsInfo
    driver_task_uid = attr.ib(default=None)

    task_run_env = attr.ib(default=None)  # type: TaskRunEnvInfo

    new_run_info = attr.ib(
        default=None
    )  # type: Optional[RunInfo]  # we create it only for the new runs
    scheduled_run_info = attr.ib(default=None)  # type: Optional[ScheduledRunInfo]

    def asdict(self):
        return attr.asdict(self, recurse=False)


class InitRunArgsSchema(ApiObjectSchema):
    run_uid = fields.UUID()
    root_run_uid = fields.UUID()

    driver_task_uid = fields.UUID(allow_none=True)

    task_run_env = fields.Nested(TaskRunEnvInfoSchema)
    task_runs_info = fields.Nested(TaskRunsInfoSchema)

    new_run_info = fields.Nested(RunInfoSchema, allow_none=True)

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


class TaskRunAttemptUpdateArgsSchema(ApiObjectSchema):
    task_run_uid = fields.UUID()
    task_run_attempt_uid = fields.UUID()
    state = EnumField(TaskRunState)
    timestamp = fields.DateTime(allow_none=True)
    error = fields.Nested(ErrorInfoSchema, allow_none=True)

    @post_load
    def make_object(self, data, **kwargs):
        return TaskRunAttemptUpdateArgs(**data)


class InitRunSchema(_ApiCallSchema):
    init_args = fields.Nested(InitRunArgsSchema)


init_run_schema = InitRunSchema()


class AddTaskRunsSchema(_ApiCallSchema):
    task_runs_info = fields.Nested(TaskRunsInfoSchema)


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


class SaveTaskRunLogSchema(_ApiCallSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    log_body = fields.String(allow_none=True)


save_task_run_log_schema = SaveTaskRunLogSchema()


class SaveExternalLinksSchema(_ApiCallSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    external_links_dict = fields.Dict(
        name=fields.Str(), url=fields.Str(), required=True
    )


save_external_links_schema = SaveExternalLinksSchema()


class LogMetricSchema(_ApiCallSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    metric = fields.Nested(MetricSchema, allow_none=True)


log_metric_schema = LogMetricSchema()


class LogArtifactSchema(_ApiCallSchema):
    task_run_attempt_uid = fields.UUID(required=True)
    name = fields.String()
    path = fields.String()


log_artifact_schema = LogArtifactSchema()


class LogTargetMetricsSchema(_ApiCallSchema):
    task_run_uid = fields.UUID(required=True)
    task_run_attempt_uid = fields.UUID(required=True)

    target_path = fields.String()

    value_preview = fields.String(allow_none=True)
    data_dimensions = fields.List(fields.Integer(), allow_none=True)
    data_schema = fields.String(allow_none=True)


log_target_metrics_schema = LogTargetMetricsSchema()


class HeartbeatSchema(_ApiCallSchema):
    run_uid = fields.UUID()


heartbeat_schema = HeartbeatSchema()


class AirflowTaskInfoSchema(_ApiCallSchema):
    execution_date = fields.DateTime()
    dag_id = fields.String()
    task_id = fields.String()
    task_run_attempt_uid = fields.UUID()
    retry_number = fields.Integer(required=False, allow_none=True)

    @post_load
    def make_object(self, data, **kwargs):
        return AirflowTaskInfo(**data)


class AirflowTaskInfosSchema(_ApiCallSchema):
    airflow_task_infos = fields.Nested(AirflowTaskInfoSchema, many=True)
    is_airflow_synced = fields.Boolean()
    base_url = fields.String()


airflow_task_infos_schema = AirflowTaskInfosSchema()


logger = logging.getLogger(__name__)


@six.add_metaclass(ABCMeta)
class TrackingAPI(object):
    def _handle(self, name, data):
        logger.info("Tracking %s.%s is not implemented", self.__class__.__name__, name)

    def init_scheduled_job(self, data):
        return self._handle(TrackingAPI.init_scheduled_job.__name__, data)

    def init_run(self, data):
        return self._handle(TrackingAPI.init_run.__name__, data)

    def add_task_runs(self, data):
        return self._handle(TrackingAPI.add_task_runs.__name__, data)

    def set_run_state(self, data):
        return self._handle(TrackingAPI.set_run_state.__name__, data)

    def set_task_reused(self, data):
        return self._handle(TrackingAPI.set_task_reused.__name__, data)

    def update_task_run_attempts(self, data):
        return self._handle(TrackingAPI.update_task_run_attempts.__name__, data)

    def save_task_run_log(self, data):
        return self._handle(TrackingAPI.save_task_run_log.__name__, data)

    def save_external_links(self, data):
        return self._handle(TrackingAPI.save_external_links.__name__, data)

    def log_target_metrics(self, data):
        return self._handle(TrackingAPI.log_target_metrics.__name__, data)

    def log_metric(self, data):
        return self._handle(TrackingAPI.log_metric.__name__, data)

    def log_artifact(self, data):
        return self._handle(TrackingAPI.log_artifact.__name__, data)

    def heartbeat(self, data):
        return self._handle(TrackingAPI.heartbeat.__name__, data)

    def save_airflow_task_infos(self, data):
        return self._handle(TrackingAPI.save_airflow_task_infos.__name__, data)


class TrackingApiClient(TrackingAPI):
    """Json API client implementation."""

    def __init__(self, api_base_url=None, auth=None):
        self.client = ApiClient(api_base_url=api_base_url, auth=auth)

    def _handle(self, name, data):
        return self.client.api_request(name, data)


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
    update_user = attr.ib(default=None)  # type: str
    update_time = attr.ib(default=None)  # type: Optional[datetime.datetime]
    from_file = attr.ib(default=False)  # type: bool
    deleted_from_file = attr.ib(default=False)  # type: bool
    list_order = attr.ib(default=None)  # type: List[int]
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


scheduled_job_args_schema = ScheduledJobArgsSchema()


@attr.s
class AirflowTaskInfo(object):
    execution_date = attr.ib()  # type: datetime.datetime
    dag_id = attr.ib()  # type: str
    task_id = attr.ib()  # type: str
    task_run_attempt_uid = attr.ib()  # type: UUID
    retry_number = attr.ib(default=None)  # type: Optional[int]

    def asdict(self):
        return dict(
            execution_date=self.execution_date,
            dag_id=self.dag_id,
            task_id=self.task_id,
            task_run_attempt_uid=self.task_run_attempt_uid,
            retry_number=self.retry_number,
        )
