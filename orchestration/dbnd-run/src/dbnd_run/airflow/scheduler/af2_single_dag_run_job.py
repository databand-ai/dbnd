# © Copyright Databand.ai, an IBM Company 2022

import typing

from airflow import DAG
from airflow.jobs.backfill_job import BackfillJob
from airflow.timetables.base import DagRunInfo
from airflow.utils.db import provide_session
from sqlalchemy.orm.session import Session


if typing.TYPE_CHECKING:
    from dbnd_run.airflow.dbnd_task_executor.dbnd_task_executor_via_airflow import (
        AirflowTaskExecutor,
    )


class SingleDagRunJob(BackfillJob):
    __mapper_args__ = {"polymorphic_identity": "BackfillJob"}

    KEY_ERROR_ON_RERUN_RETRY = 100

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
        for i in range(self.KEY_ERROR_ON_RERUN_RETRY):
            try:
                super()._execute_dagruns(
                    dagrun_infos=dagrun_infos,
                    ti_status=ti_status,
                    executor=executor,
                    pickle_id=self.dag.pickle_id,  # THIS IS INJECTION, we always have pickle ID!
                    start_date=start_date,
                    session=session,
                )

                # on success - return, otherwise we are retrying
                return
            except KeyError as ex:
                # Fix the error from try_number racing issue: KeyError TaskInstanceKey(dag_id=.. )
                # https://github.com/apache/airflow/pull/30669
                # https://github.com/apache/airflow/issues/13322
                # It has been fixed in 2.6.2 (may be)
                # the last fix is still not released.
                #  File  airflow/jobs/backfill_job.py", line 631, in _process_backfill_task_instances  self._update_counters(ti_status=ti_status, session=session)
                #  File "airflow/jobs/backfill_job.py", line 182, in _update_counters   ti_status.running.pop(reduced_key)
                #
                # we will try to handle it via skipping the error, so in the next iteration the task is going to be
                # checked and proceed
                if i == self.KEY_ERROR_ON_RERUN_RETRY - 1:
                    self.log.warning(
                        "KeyError in _update_counters: failed to retry, continue (most likely fail with failed jobs check)"
                    )
                    raise

                self.log.warning(
                    "KeyError in _update_counters: skipping this iteration, "
                    "the issue should be resolved in the next iteration (retry %s out of %s retries): %s",
                    i,
                    self.KEY_ERROR_ON_RERUN_RETRY,
                    ex,
                )

    def _log_progress(self, ti_status):
        self.dbnd_airflow_executor.handle_process_dag_task_instanced_iteration(
            ti_status=ti_status
        )
        super()._log_progress(ti_status=ti_status)
