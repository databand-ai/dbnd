# © Copyright Databand.ai, an IBM Company 2022

import typing

from airflow import DAG
from airflow.jobs.backfill_job import BackfillJob
from airflow.timetables.base import DagRunInfo
from airflow.utils.session import provide_session
from sqlalchemy.orm.session import Session


if typing.TYPE_CHECKING:
    from dbnd_run.airflow.dbnd_task_executor.dbnd_task_executor_via_airflow import (
        AirflowTaskExecutor,
    )


class SingleDagRunJob(BackfillJob):
    __mapper_args__ = {"polymorphic_identity": "BackfillJob"}

    def __getstate__(self):
        state = self.__dict__.copy()
        # Don't pickle baz
        del state["dag"]
        return state

    #
    # def _execute(self, session=None):
    #
    #     return super()._execute(session=session)
    #
    #
    # def heartbeat(self, *args, **kwargs):
    #     # sync the attempt with the retries
    #     return super().heartbeat( *args, **kwargs)

    @property
    def dbnd_airflow_executor(self) -> "AirflowTaskExecutor":
        # loopback to Databand executor
        from dbnd_run.current import get_run_executor

        return get_run_executor().task_executor

    @provide_session
    def _get_dag_run(self, dagrun_info: DagRunInfo, dag: DAG, session: Session = None):
        return self.dbnd_airflow_executor.dag_run

    @provide_session
    def _execute_dagruns(
        self, dagrun_infos, ti_status, executor, pickle_id, start_date, session=None
    ):
        # we need to provide pickle_id
        return super()._execute_dagruns(
            dagrun_infos=dagrun_infos,
            ti_status=ti_status,
            executor=executor,
            pickle_id=self.dag.pickle_id,  # THIS IS INJECTION, we always have pickle ID!
            start_date=start_date,
            session=session,
        )

    def _log_progress(self, ti_status):
        self.dbnd_airflow_executor.handle_process_dag_task_instanced_iteration(
            ti_status=ti_status
        )
        super()._log_progress(ti_status=ti_status)
