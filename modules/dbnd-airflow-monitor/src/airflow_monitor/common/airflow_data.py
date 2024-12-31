# © Copyright Databand.ai, an IBM Company 2022
from typing import List, Optional

import attr

from dbnd_airflow.export_plugin.models import DagRunState
from dbnd_monitor.base_monitor_config import NOTHING, BaseMonitorState


@attr.s
class LastSeenValues:
    last_seen_dag_run_id = attr.ib()  # type: Optional[int]

    def as_dict(self):
        return dict(last_seen_dag_run_id=self.last_seen_dag_run_id)

    @classmethod
    def from_dict(cls, data):
        return cls(last_seen_dag_run_id=data.get("last_seen_dag_run_id"))


@attr.s
class PluginMetadata:
    airflow_version = attr.ib(default=None)
    plugin_version = attr.ib(default=None)
    airflow_instance_uid = attr.ib(default=None)
    api_mode = attr.ib(default=None)

    def as_dict(self):
        return dict(
            airflow_version=self.airflow_version,
            plugin_version=self.plugin_version,
            airflow_instance_uid=self.airflow_instance_uid,
            api_mode=self.api_mode,
        )

    def as_safe_dict(self):
        values_dict = {
            key: "" if value is NOTHING else value
            for key, value in self.as_dict().items()
        }
        return values_dict

    @classmethod
    def from_dict(cls, data):
        return cls(
            airflow_version=data.get("airflow_version"),
            plugin_version=data.get("plugin_version"),
            airflow_instance_uid=data.get("airflow_instance_uid"),
            api_mode=data.get("api_mode"),
        )


@attr.s
class MonitorState(BaseMonitorState):
    airflow_version = attr.ib(default=NOTHING)
    airflow_export_version = attr.ib(default=NOTHING)
    airflow_monitor_version = attr.ib(default=NOTHING)
    monitor_status = attr.ib(default=NOTHING)
    monitor_error_message = attr.ib(default=NOTHING)
    airflow_instance_uid = attr.ib(default=NOTHING)
    api_mode = attr.ib(default=NOTHING)


@attr.s
class AirflowDagRun:
    id = attr.ib()  # type: int
    dag_id = attr.ib()  # type: str
    execution_date = attr.ib()  # type: str
    state = attr.ib()  # type: DagRunState
    is_paused = attr.ib()  # type: bool


@attr.s
class AirflowDagRunsResponse:
    dag_runs = attr.ib()  # type: List[AirflowDagRun]
    last_seen_dag_run_id = attr.ib()  # type: Optional[int]

    @classmethod
    def from_dict(cls, data):
        return cls(
            dag_runs=[
                AirflowDagRun(
                    id=dr.get("id"),
                    dag_id=dr.get("dag_id"),
                    execution_date=dr.get("execution_date"),
                    state=dr.get("state"),
                    is_paused=dr.get("is_paused"),
                )
                for dr in data.get("new_dag_runs") or []
            ],
            last_seen_dag_run_id=data.get("last_seen_dag_run_id"),
        )


@attr.s
class DagRunsFullData:
    dags = attr.ib()
    dag_runs = attr.ib()
    task_instances = attr.ib()

    def as_dict(self):
        return dict(
            dags=self.dags, dag_runs=self.dag_runs, task_instances=self.task_instances
        )

    @classmethod
    def from_dict(cls, data):
        return cls(
            dags=list(data.get("dags")),
            dag_runs=list(data.get("dag_runs")),
            task_instances=list(data.get("task_instances")),
        )


@attr.s
class DagRunsStateData:
    dag_runs = attr.ib()
    task_instances = attr.ib()

    def as_dict(self):
        return dict(task_instances=self.task_instances, dag_runs=self.dag_runs)

    @classmethod
    def from_dict(cls, data):
        return cls(
            task_instances=list(data.get("task_instances")),
            dag_runs=list(data.get("dag_runs")),
        )
