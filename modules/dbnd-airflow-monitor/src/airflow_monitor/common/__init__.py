from typing import List, Optional
from uuid import UUID

import attr


@attr.s
class DagRunToSync(object):
    dagrun_id = attr.ib()
    dag_id = attr.ib()
    execution_date = attr.ib()


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
class DbndDagRunsResponse:
    dag_run_ids = attr.ib()  # type: List[int]
    last_seen_dag_run_id = attr.ib()  # type: Optional[int]
    last_seen_log_id = attr.ib()  # type: Optional[int]

    @classmethod
    def from_dict(cls, response):
        return cls(
            dag_run_ids=response["dag_run_ids"],
            last_seen_dag_run_id=response["last_seen_dag_run_id"],
            last_seen_log_id=response["last_seen_log_id"],
        )


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
                for dr in data.get("new_dag_runs")
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
            dags=[dag for dag in data.get("dags")],
            dag_runs=[dag_run for dag_run in data.get("dag_runs")],
            task_instances=[
                task_instance for task_instance in data.get("task_instances")
            ],
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
            task_instances=[
                task_instance for task_instance in data.get("task_instances")
            ],
            dag_runs=[dag_run for dag_run in data.get("dag_runs")],
        )


# configurations
@attr.s
class AirflowServerConfig(object):
    tracking_source_uid = attr.ib()  # type: UUID
    state_sync_enabled = attr.ib(default=False)  # type: bool
    xcom_sync_enabled = attr.ib(default=False)  # type: bool
    dag_sync_enabled = attr.ib(default=False)  # type: bool
    fixer_enabled = attr.ib(default=False)  # type: bool

    is_sync_enabled = attr.ib(default=True)  # type: bool
    base_url = attr.ib(default=None)  # type: str
    api_mode = attr.ib(default=None)  # type: str
    fetcher = attr.ib(default="web")  # type: str

    # for web data fetcher
    rbac_username = attr.ib(default=None)  # type: str
    rbac_password = attr.ib(default=None)  # type: str

    # for composer data fetcher
    composer_client_id = attr.ib(default=None)  # type: str

    # for db data fetcher
    local_dag_folder = attr.ib(default=None)  # type: str
    sql_alchemy_conn = attr.ib(default=None)  # type: str

    # for file data fetcher
    json_file_path = attr.ib(default=None)  # type: str

    @classmethod
    def create(cls, airflow_config, server_config):
        monitor_config = server_config.get("monitor_config") or {}
        kwargs = {k: v for k, v in monitor_config.items() if k in attr.fields_dict(cls)}

        conf = cls(
            tracking_source_uid=server_config["tracking_source_uid"],
            is_sync_enabled=server_config["is_sync_enabled"],
            base_url=server_config["base_url"],
            api_mode=server_config["api_mode"],
            fetcher=server_config["fetcher"],
            composer_client_id=server_config["composer_client_id"],
            sql_alchemy_conn=airflow_config.sql_alchemy_conn,  # TODO: currently support only one server!
            json_file_path=airflow_config.json_file_path,  # TODO: currently support only one server!
            rbac_username=airflow_config.rbac_username,  # TODO: currently support only one server!
            rbac_password=airflow_config.rbac_password,  # TODO: currently support only one server!
            **kwargs,
        )
        return conf


@attr.s
class MonitorConfig(AirflowServerConfig):
    init_dag_run_bulk_size = attr.ib(default=10)  # type: int

    max_execution_date_window = attr.ib(default=14)  # type: int
    interval = attr.ib(default=10)  # type: int


@attr.s
class MultiServerMonitorConfig:
    interval = attr.ib(default=0)
    runner_type = attr.ib(default="seq")  # seq/mp
    number_of_iterations = attr.ib(default=None)  # type: Optional[int]
    tracking_source_uids = attr.ib(
        default=None, converter=lambda v: list(v) if v else v,
    )  # type: Optional[List[UUID]]


@attr.s
class TrackingServiceConfig:
    url = attr.ib()
    access_token = attr.ib(default=None)
    user = attr.ib(default=None)
    password = attr.ib(default=None)
