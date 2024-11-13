# © Copyright Databand.ai, an IBM Company 2022


from typing import List, Optional

import attr

from airflow_monitor.common.airflow_data import (
    AirflowDagRun,
    AirflowDagRunsResponse,
    DagRunsFullData,
    DagRunsStateData,
    LastSeenValues,
)
from airflow_monitor.data_fetcher.db_data_fetcher import DbFetcher
from test_dbnd_airflow_monitor.airflow_utils import can_be_dead


@attr.s
class MockDagRun:
    id = attr.ib()  # type: int
    dag_id = attr.ib(default="dag1")  # type: str
    execution_date = attr.ib(default="date1")  # type: str
    state = attr.ib(default="RUNNING")  # type: str
    is_paused = attr.ib(default=False)  # type: bool
    max_log_id = attr.ib(default=None)  # type: Optional[int]
    events = attr.ib(default=None)  # type: str

    # mock only
    n_task_instances = attr.ib(default=3)

    # mock only
    test_created_at = attr.ib(default=None)
    test_updated_at = attr.ib(default=None)

    def as_dict(self):
        return {
            "id": self.id,
            "dag_id": self.dag_id,
            "execution_date": self.execution_date,
            "state": self.state,
            "is_paused": self.is_paused,
            "max_log_id": self.max_log_id,
            "events": self.events,
        }


@attr.s
class MockTaskInstance:
    dag_id = attr.ib(default="dag1")  # type: str
    execution_date = attr.ib(default="date1")  # type: str

    def as_dict(self):
        return {"dag_id": self.dag_id, "execution_date": self.execution_date}


@attr.s
class MockLog:
    id = attr.ib()  # type: int
    dag_id = attr.ib(default="dag1")  # type: str
    execution_date = attr.ib(default="date1")  # type: str


class MockDataFetcher(DbFetcher):
    def __init__(self):
        self.source_name = "test"
        self.dag_runs = []  # type: List[MockDagRun]
        self.logs = []  # type: List[MockLog]
        self.alive = True

    @can_be_dead
    def get_last_seen_values(self) -> LastSeenValues:
        return LastSeenValues(
            last_seen_dag_run_id=max(dr.id for dr in self.dag_runs)
            if self.dag_runs
            else None,
            last_seen_log_id=max(log.id for log in self.logs) if self.logs else None,
        )

    @can_be_dead
    def get_airflow_dagruns_to_sync(
        self,
        last_seen_dag_run_id: Optional[int] = None,
        last_seen_log_id: Optional[int] = None,
        extra_dag_run_ids: Optional[List[int]] = None,
        dag_ids: Optional[str] = None,
        excluded_dag_ids: Optional[str] = None,
    ) -> AirflowDagRunsResponse:
        dag_ids_list = dag_ids.split(",") if dag_ids else []

        updated = {}
        if last_seen_log_id is not None:
            for log in self.logs:
                if log.id > last_seen_log_id:
                    key = (log.dag_id, log.execution_date)
                    updated[key] = max(log.id, updated.get(key, -1))

        dag_runs = [
            AirflowDagRun(
                id=dr.id,
                dag_id=dr.dag_id,
                execution_date=dr.execution_date,
                state=dr.state,
                is_paused=dr.is_paused,
                has_updated_task_instances=(dr.dag_id, dr.execution_date) in updated,
                max_log_id=updated.get((dr.dag_id, dr.execution_date)),
            )
            for dr in self.dag_runs
            if (
                (
                    (dr.state == "RUNNING" and not dr.is_paused)
                    or dr.id in extra_dag_run_ids
                    or (
                        last_seen_dag_run_id is not None
                        and dr.id > last_seen_dag_run_id
                    )
                    or (dr.dag_id, dr.execution_date) in updated
                )
                and (not dag_ids_list or dr.dag_id in dag_ids_list)
            )
        ]
        return AirflowDagRunsResponse(
            dag_runs=dag_runs,
            last_seen_dag_run_id=max(dr.id for dr in self.dag_runs)
            if self.dag_runs
            else None,
            last_seen_log_id=max(log.id for log in self.logs) if self.logs else None,
        )

    @can_be_dead
    def get_full_dag_runs(
        self, dag_run_ids: List[int], include_sources: bool
    ) -> DagRunsFullData:
        dag_run_ids = list(dag_run_ids)
        return DagRunsFullData(
            dags=[],
            dag_runs=[dr for dr in self.dag_runs if dr.id in dag_run_ids],
            task_instances=[
                MockTaskInstance(dag_id=dr.dag_id, execution_date=dr.execution_date)
                for dr in self.dag_runs
                if dr.id in dag_run_ids
                for _ in range(dr.n_task_instances)
            ],
        )

    @can_be_dead
    def get_dag_runs_state_data(self, dag_run_ids: List[int]) -> DagRunsStateData:
        dag_run_ids = set(dag_run_ids)
        return DagRunsStateData(
            dag_runs=[dr for dr in self.dag_runs if dr.id in dag_run_ids],
            task_instances=[
                MockTaskInstance(dag_id=dr.dag_id, execution_date=dr.execution_date)
                for dr in self.dag_runs
                if dr.id in dag_run_ids
                for _ in range(dr.n_task_instances)
            ],
        )

    def is_alive(self):
        pass
