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

    name = attr.ib()  # type: str
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
    task_run_uid = attr.ib()  # type: UUID
    task_run_attempt_uid = attr.ib()  # type: UUID

    task_definition_uid = attr.ib()  # type: UUID
    run_uid = attr.ib()  # type: UUID

    execution_date = attr.ib()  # type: datetime
    task_af_id = attr.ib()  # type: str

    task_id = attr.ib()  # type: str
    task_signature = attr.ib()  # type: str

    name = attr.ib()  # type: str

    env = attr.ib()  # type: str

    command_line = attr.ib()  # type: str
    functional_call = attr.ib()  # type: str

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
