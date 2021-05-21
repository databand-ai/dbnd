import logging
import typing

import attr

from dbnd._core.constants import TaskRunState, _DbndDataClass


if typing.TYPE_CHECKING:
    from typing import Optional, List
    from datetime import datetime, date
    from uuid import UUID
    from dbnd._core.parameter.parameter_definition import ParameterDefinition


logger = logging.getLogger(__name__)


@attr.s
class TaskDefinitionInfo(_DbndDataClass):
    task_definition_uid = attr.ib()  # type: UUID

    name = attr.ib(repr=True)  # type: str
    family = attr.ib()  # type: str
    type = attr.ib()  # type: str

    class_version = attr.ib()  # type: str

    task_param_definitions = attr.ib()  # type: List[ParameterDefinition]

    module_source = attr.ib(default=None)  # type: str
    module_source_hash = attr.ib(default=None)  # type: str

    source = attr.ib(default=None)  # type: str
    source_hash = attr.ib(default=None)  # type: str

    def __repr__(self):
        return "TaskDefinitionInfo(%s)" % self.name


@attr.s
class TaskRunParamInfo(_DbndDataClass):
    parameter_name = attr.ib()  # type: str
    value = attr.ib()  # type: str

    value_origin = attr.ib()  # type: str


@attr.s
class TaskRunInfo(_DbndDataClass):
    task_run_uid = attr.ib(repr=False)  # type: UUID
    task_run_attempt_uid = attr.ib(repr=False)  # type: UUID

    task_definition_uid = attr.ib(repr=False)  # type: UUID
    run_uid = attr.ib(repr=False)  # type: UUID

    task_af_id = attr.ib()  # type: str
    task_id = attr.ib()  # type: str

    execution_date = attr.ib()  # type: datetime
    task_signature = attr.ib()  # type: str

    name = attr.ib()  # type: str

    env = attr.ib()  # type: str

    command_line = attr.ib(repr=False)  # type: str
    functional_call = attr.ib(repr=False)  # type: str

    has_downstreams = attr.ib()  # type: bool
    has_upstreams = attr.ib()  # type: bool

    is_reused = attr.ib()  # type: bool
    is_skipped = attr.ib()  # type: bool

    is_dynamic = attr.ib()  # type: bool
    is_system = attr.ib()  # type: bool

    is_root = attr.ib()  # type: bool

    output_signature = attr.ib()  # type: str
    state = attr.ib()  # type: TaskRunState
    target_date = attr.ib()  # type: Optional[date]

    version = attr.ib()  # type: str

    log_local = attr.ib()  # type: str
    log_remote = attr.ib()  # type: str

    task_run_params = attr.ib()  # type: List[TaskRunParamInfo]

    task_signature_source = attr.ib(default=None)  # type: str

    external_links = attr.ib(default=None)  # type: dict

    # This property allows to separate info that was created from a real task instance from an info that was created
    # for a task in the task graph when there was no task instance corresponding to it.
    # See the use of _to_task_run_info in airflow_monitor_converting.py
    is_dummy = attr.ib(default=False)  # type: bool

    def __repr__(self):
        return "TaskRunInfo(%s, %s)" % self.name, self.state


@attr.s(hash=True)
class TargetInfo(_DbndDataClass):
    parameter_name = attr.ib()  # type: str
    path = attr.ib(hash=True)  # type: str
    created_date = attr.ib()  # type: Optional[datetime]
    task_run_uid = attr.ib()  # type: Optional[UUID]


@attr.s
class ErrorInfo(_DbndDataClass):
    msg = attr.ib()  # type: str
    help_msg = attr.ib()  # type: str
    exc_type = attr.ib()  # type: str
    databand_error = attr.ib()  # type: bool
    traceback = attr.ib()  # type: str
    nested = attr.ib()  # type: str
    user_code_traceback = attr.ib()  # type: str
    show_exc_info = attr.ib()  # type: bool


@attr.s
class TaskRunEnvInfo(_DbndDataClass):
    uid = attr.ib()  # type: UUID

    cmd_line = attr.ib()  # type: str
    databand_version = attr.ib()  # type: str
    user_code_version = attr.ib()  # type: str
    user_code_committed = attr.ib()  # type: bool
    project_root = attr.ib()  # type: str

    user_data = attr.ib()  # type: str

    user = attr.ib()  # type: str
    machine = attr.ib()  # type: str

    heartbeat = attr.ib()  # type: datetime
