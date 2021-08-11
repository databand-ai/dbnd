import typing

from datetime import datetime
from typing import List

import attr

from airflow.models import BaseOperator, DagRun, TaskInstance
from airflow.utils.net import get_hostname

from dbnd._core.utils.uid_utils import source_md5
from dbnd_airflow_export.helpers import (
    _extract_args_from_dict,
    _get_command_from_operator,
    _get_module_code,
    _get_source_code,
    _read_dag_file,
    interval_to_str,
    resolve_attribute_or_default_attribute,
    resolve_attribute_or_default_value,
)


if typing.TYPE_CHECKING:
    from typing import List
    from airflow.models import DagModel, DagTag, DAG


class ETask(object):
    def __init__(
        self,
        upstream_task_ids=None,
        downstream_task_ids=None,
        task_type=None,
        task_source_code=None,
        task_source_hash=None,
        task_module_code=None,
        module_source_hash=None,
        dag_id=None,
        task_id=None,
        retries=None,
        command=None,
        task_args=None,
    ):
        self.upstream_task_ids = list(upstream_task_ids)  # type: List[str]
        self.downstream_task_ids = list(downstream_task_ids)  # type: List[str]
        self.task_type = task_type
        self.task_source_code = task_source_code
        self.task_source_hash = task_source_hash
        self.task_module_code = task_module_code
        self.module_source_hash = module_source_hash
        self.dag_id = dag_id
        self.task_id = task_id
        self.retries = retries
        self.command = command
        self.task_args = task_args

    @staticmethod
    def from_task(t, include_task_args, dag, include_source=True):
        # type: (BaseOperator, bool, DAG, bool) -> ETask
        module_code = _get_module_code(t) or _read_dag_file(dag.fileloc)
        return ETask(
            upstream_task_ids=t.upstream_task_ids,
            downstream_task_ids=t.downstream_task_ids,
            task_type=t.task_type,
            task_source_code=_get_source_code(t) if include_source else None,
            task_source_hash=source_md5(_get_source_code(t)),
            task_module_code=module_code if include_source else None,
            module_source_hash=source_md5(module_code),
            dag_id=t.dag_id,
            task_id=t.task_id,
            retries=t.retries,
            command=_get_command_from_operator(t),
            task_args=_extract_args_from_dict(vars(t)) if include_task_args else {},
        )

    def as_dict(self):
        return dict(
            upstream_task_ids=self.upstream_task_ids,
            downstream_task_ids=self.downstream_task_ids,
            task_type=self.task_type,
            task_source_code=self.task_source_code,
            task_source_hash=self.task_source_hash,
            task_module_code=self.task_module_code,
            module_source_hash=self.module_source_hash,
            dag_id=self.dag_id,
            task_id=self.task_id,
            retries=self.retries,
            command=self.command,
            task_args=self.task_args,
        )


class EDagRun(object):
    db_fields = [
        "dag_id",
        "id",
        "start_date",
        "state",
        "end_date",
        "execution_date",
        "conf",
        "run_id",
    ]

    @classmethod
    def query_fields(cls):
        return [getattr(DagRun, key) for key in cls.db_fields]

    def __init__(
        self,
        dag_id,
        dagrun_id,
        start_date,
        state,
        end_date,
        execution_date,
        task_args,
        run_id,
    ):
        self.dag_id = dag_id
        self.dagrun_id = dagrun_id
        self.start_date = start_date
        self.state = state
        self.end_date = end_date
        self.execution_date = execution_date
        self.task_args = task_args
        self.run_id = run_id

    @classmethod
    def from_db_fields(
        cls,
        dag_id,
        dagrun_id,
        start_date,
        state,
        end_date,
        execution_date,
        conf,
        run_id,
    ):
        return cls(
            dag_id,
            dagrun_id,
            start_date,
            state,
            end_date,
            execution_date,
            (_extract_args_from_dict(conf) if conf else {}),
            run_id,
        )

    def __hash__(self):
        return hash(self.dagrun_id)

    def __eq__(self, other):
        return isinstance(other, EDagRun) and self.dagrun_id == other.dagrun_id

    def as_dict(self):
        return dict(
            dag_id=self.dag_id,
            dagrun_id=self.dagrun_id,
            start_date=self.start_date,
            state=self.state,
            end_date=self.end_date,
            execution_date=self.execution_date,
            task_args=self.task_args,
            run_id=self.run_id,
        )


class EDag(object):
    def __init__(
        self,
        description,
        root_task_ids,
        tasks,
        owner,
        dag_id,
        schedule_interval,
        catchup,
        start_date,
        end_date,
        dag_folder,
        hostname,
        source_code,
        module_source_hash,
        is_subdag,
        task_type,
        task_args,
        is_active,
        is_paused,
        git_commit,
        is_committed,
        tags,
    ):
        self.description = description
        self.root_task_ids = root_task_ids  # type: List[str]
        self.tasks = tasks  # type: List[ETask]
        self.tags = tags  # type: List[DagTag]
        self.owner = owner
        self.dag_id = dag_id
        self.schedule_interval = schedule_interval
        self.catchup = catchup
        self.start_date = start_date
        self.end_date = end_date
        self.dag_folder = dag_folder
        self.hostname = hostname
        self.source_code = source_code
        self.module_source_hash = module_source_hash
        self.is_subdag = is_subdag
        self.task_type = task_type
        self.task_args = task_args
        self.is_active = is_active
        self.is_paused = is_paused
        self.git_commit = git_commit
        self.is_committed = is_committed

    @staticmethod
    def from_dag(
        dag,
        dm,
        dag_folder,
        include_task_args,
        git_commit,
        is_committed,
        raw_data_only=False,
        include_source=True,
    ):
        # type: (DAG, DagModel, str, bool, str, bool, bool, bool) -> EDag
        # Can be Dag from DagBag or from DB, therefore not all attributes may exist
        source_code = _read_dag_file(dag.fileloc)
        return EDag(
            description=dag.description or "",
            root_task_ids=[t.task_id for t in getattr(dag, "roots", [])],
            tasks=[
                ETask.from_task(t, include_task_args, dag, include_source)
                for t in getattr(dag, "tasks", [])
            ]
            if not raw_data_only
            else [],
            owner=resolve_attribute_or_default_attribute(dag, ["owner", "owners"]),
            dag_id=dag.dag_id,
            schedule_interval=interval_to_str(dag.schedule_interval),
            catchup=resolve_attribute_or_default_value(dag, "catchup", False),
            start_date=resolve_attribute_or_default_value(dag, "start_date", None),
            end_date=resolve_attribute_or_default_value(dag, "end_date", None),
            dag_folder=dag_folder,
            hostname=get_hostname(),
            source_code=source_code if not raw_data_only and include_source else "",
            module_source_hash=source_md5(source_code),
            is_subdag=dag.is_subdag,
            tags=getattr(dm, "tags", []),
            task_type="DAG",
            task_args=_extract_args_from_dict(vars(dag)) if include_task_args else {},
            is_active=dm.is_active,
            is_paused=dm.is_paused,
            git_commit=git_commit,
            is_committed=is_committed,
        )

    def as_dict(self):
        return dict(
            description=self.description,
            root_task_ids=self.root_task_ids,
            tasks=[t.as_dict() for t in self.tasks],
            tags=[tag.name for tag in self.tags],
            owner=self.owner,
            dag_id=self.dag_id,
            schedule_interval=self.schedule_interval,
            catchup=self.catchup,
            start_date=self.start_date,
            end_date=self.end_date,
            is_committed=self.is_committed,
            git_commit=self.git_commit,
            dag_folder=self.dag_folder,
            hostname=self.hostname,
            source_code=self.source_code,
            module_source_hash=self.module_source_hash,
            is_subdag=self.is_subdag,
            task_type=self.task_type,
            task_args=self.task_args,
        )


@attr.s
class AirflowNewDagRun(object):
    id = attr.ib()  # type: int
    dag_id = attr.ib()  # type: str
    execution_date = attr.ib()  # type: datetime
    state = attr.ib()  # type: str
    is_paused = attr.ib()  # type: bool
    has_updated_task_instances = attr.ib()  # type: bool
    max_log_id = attr.ib()  # type: int
    events = attr.ib()  # type: List[str]

    def as_dict(self):
        return dict(
            id=self.id,
            dag_id=self.dag_id,
            execution_date=self.execution_date,
            state=self.state,
            is_paused=self.is_paused,
            has_updated_task_instances=self.has_updated_task_instances,
            max_log_id=self.max_log_id,
            events=self.events,
        )


class AirflowTaskInstance(object):
    def __init__(
        self, dag_id, task_id, execution_date, state, try_number, start_date, end_date,
    ):
        self.execution_date = execution_date
        self.dag_id = dag_id
        self.state = state
        self.try_number = try_number
        self.task_id = task_id
        self.start_date = start_date
        self.end_date = end_date

    db_fields = [
        "dag_id",
        "task_id",
        "execution_date",
        "state",
        "_try_number",
        "start_date",
        "end_date",
    ]

    @classmethod
    def query_fields(cls):
        return [getattr(TaskInstance, key) for key in cls.db_fields]

    def as_dict(self):
        return dict(
            dag_id=self.dag_id,
            task_id=self.task_id,
            execution_date=self.execution_date,
            state=self.state,
            try_number=self.try_number,
            start_date=self.start_date,
            end_date=self.end_date,
        )


@attr.s
class AirflowExportMeta(object):
    airflow_version = attr.ib(default=None)  # type: str
    plugin_version = attr.ib(default=None)  # type: str
    airflow_instance_uid = attr.ib(default=None)  # type: str
    request_args = attr.ib(default=None)  # type: dict
    metrics = attr.ib(default=None)  # type: dict

    def as_dict(self):
        return dict(
            airflow_version=self.airflow_version,
            plugin_version=self.plugin_version,
            airflow_instance_uid=self.airflow_instance_uid,
            request_args=self.request_args,
            metrics=self.metrics,
        )


@attr.s
class AirflowExportData(object):
    airflow_export_meta = attr.ib(default=None)  # type: AirflowExportMeta
    error_message = attr.ib(default=None)  # type: str

    def as_dict(self):
        return dict(
            airflow_export_meta=self.airflow_export_meta.as_dict(),
            error_message=self.error_message,
        )


@attr.s
class LastSeenData(AirflowExportData):
    last_seen_dag_run_id = attr.ib(default=None)  # type: int
    last_seen_log_id = attr.ib(default=None)  # type: int

    def as_dict(self):
        return dict(
            last_seen_dag_run_id=self.last_seen_dag_run_id,
            last_seen_log_id=self.last_seen_log_id,
            airflow_export_meta=self.airflow_export_meta.as_dict(),
            error_message=self.error_message,
        )


@attr.s
class NewRunsData(AirflowExportData):
    new_dag_runs = attr.ib(default=None)  # type: List[AirflowNewDagRun]
    last_seen_dag_run_id = attr.ib(default=None)  # type: int
    last_seen_log_id = attr.ib(default=None)  # type: int

    def as_dict(self):
        return dict(
            new_dag_runs=[new_dag_run.as_dict() for new_dag_run in self.new_dag_runs],
            last_seen_dag_run_id=self.last_seen_dag_run_id,
            last_seen_log_id=self.last_seen_log_id,
            airflow_export_meta=self.airflow_export_meta.as_dict(),
            error_message=self.error_message,
        )


@attr.s
class FullRunsData(AirflowExportData):
    task_instances = attr.ib(default=None)  # type: List[AirflowTaskInstance]
    dag_runs = attr.ib(default=None)  # type: List[EDagRun]
    dags = attr.ib(default=None)  # type: List[EDag]

    def as_dict(self):
        return dict(
            task_instances=[
                task_instance.as_dict() for task_instance in self.task_instances
            ],
            dag_runs=[run.as_dict() for run in self.dag_runs],
            dags=[dag.as_dict() for dag in self.dags],
            airflow_export_meta=self.airflow_export_meta.as_dict(),
            error_message=self.error_message,
        )


@attr.s
class DagRunsStatesData(AirflowExportData):
    task_instances = attr.ib(default=None)  # type: List[AirflowTaskInstance]
    dag_runs = attr.ib(default=None)  # type: List[EDagRun]

    def as_dict(self):
        return dict(
            task_instances=[
                task_instance.as_dict() for task_instance in self.task_instances
            ],
            dag_runs=[run.as_dict() for run in self.dag_runs],
            airflow_export_meta=self.airflow_export_meta.as_dict(),
            error_message=self.error_message,
        )
