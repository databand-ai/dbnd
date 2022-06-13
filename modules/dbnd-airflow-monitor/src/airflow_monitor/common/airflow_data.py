from typing import List, Optional

import attr

from dbnd._core.utils.basics.nothing import NOTHING


@attr.s
class LastSeenValues:
    last_seen_dag_run_id = attr.ib()  # type: Optional[int]
    last_seen_log_id = attr.ib()  # type: Optional[int]

    def as_dict(self):
        return dict(
            last_seen_dag_run_id=self.last_seen_dag_run_id,
            last_seen_log_id=self.last_seen_log_id,
        )

    @classmethod
    def from_dict(cls, data):
        return cls(
            last_seen_dag_run_id=data.get("last_seen_dag_run_id"),
            last_seen_log_id=data.get("last_seen_log_id"),
        )


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
            is_rbac_enabled=self.api_mode,
        )

    @classmethod
    def from_dict(cls, data):
        return cls(
            airflow_version=data.get("airflow_version"),
            plugin_version=data.get("plugin_version"),
            airflow_instance_uid=data.get("airflow_instance_uid"),
            api_mode=data.get("api_mode"),
        )


@attr.s
class MonitorState:
    airflow_version = attr.ib(default=NOTHING)
    airflow_export_version = attr.ib(default=NOTHING)
    airflow_monitor_version = attr.ib(default=NOTHING)
    monitor_status = attr.ib(default=NOTHING)
    monitor_error_message = attr.ib(default=NOTHING)
    airflow_instance_uid = attr.ib(default=NOTHING)
    api_mode = attr.ib(default=NOTHING)

    def as_dict(self):
        # don't serialize data which didn't changed: as_dict should be able to return
        # None value when it set, specifically for monitor_error_message - when not set
        # at all (=NOTHING, not changing) - no need to pass serialize it, vs set to None
        # (means is changed and is None=empty) - serialize as None
        d = dict(
            airflow_version=self.airflow_version,
            airflow_export_version=self.airflow_export_version,
            airflow_monitor_version=self.airflow_monitor_version,
            monitor_status=self.monitor_status,
            monitor_error_message=self.monitor_error_message,
            airflow_instance_uid=self.airflow_instance_uid,
            api_mode=self.api_mode,
        )
        # allow partial dump
        return {k: v for k, v in d.items() if v is not NOTHING}


@attr.s
class AirflowDagRun:
    id = attr.ib()  # type: int
    dag_id = attr.ib()  # type: str
    execution_date = attr.ib()  # type: str
    state = attr.ib()  # type: str
    is_paused = attr.ib()  # type: bool
    has_updated_task_instances = attr.ib()  # type: bool
    max_log_id = attr.ib()  # type: int
    events = attr.ib(default=None)  # type: str


@attr.s
class AirflowDagRunsResponse:
    dag_runs = attr.ib()  # type: List[AirflowDagRun]
    last_seen_dag_run_id = attr.ib()  # type: Optional[int]
    last_seen_log_id = attr.ib()  # type: Optional[int]

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
                    has_updated_task_instances=dr.get("has_updated_task_instances"),
                    max_log_id=dr.get("max_log_id"),
                )
                for dr in data.get("new_dag_runs") or []
            ],
            last_seen_dag_run_id=data.get("last_seen_dag_run_id"),
            last_seen_log_id=data.get("last_seen_log_id"),
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
