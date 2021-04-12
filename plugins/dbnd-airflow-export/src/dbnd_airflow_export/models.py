from datetime import datetime
from typing import List

import attr

from airflow.models import TaskInstance

from dbnd_airflow_export.plugin_old.model import EDag, EDagRun


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
    request_args = attr.ib(default=None)  # type: dict
    metrics = attr.ib(default=None)  # type: dict

    def as_dict(self):
        return dict(
            airflow_version=self.airflow_version,
            plugin_version=self.plugin_version,
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
