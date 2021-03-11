import datetime
import typing

import airflow

from airflow import LoggingMixin, conf
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.state import State
from sqlalchemy import or_

from dbnd._core.log.logging_utils import PrefixLoggerAdapter


if typing.TYPE_CHECKING:
    from dbnd_airflow.executors.kubernetes_executor.kubernetes_executor import (
        DbndKubernetesExecutor,
    )


class ClearKubernetesRuntimeZombiesForDagRun(LoggingMixin):
    """
    workaround for SingleDagRunJob to clean "missing" tasks
    for now it works for kubernetes only,
    as k8s executor requires "explicit" _clean_state for "zombie" tasks
    otherwise, they will not be submitted (as they are still at executior.running tasks
    """

    def __init__(self, k8s_executor):
        super(ClearKubernetesRuntimeZombiesForDagRun, self).__init__()
        self.zombie_threshold_secs = conf.getint(
            "scheduler", "scheduler_zombie_task_threshold"
        )
        self.zombie_query_interval_secs = 10
        self._last_zombie_query_time = None
        self.k8s_executor = k8s_executor  # type: DbndKubernetesExecutor
        self._log = PrefixLoggerAdapter("clear-zombies", self.log)

    @provide_session
    def find_and_clean_dag_zombies(self, dag, execution_date, session):

        now = timezone.utcnow()
        if (
            self._last_zombie_query_time
            and (now - self._last_zombie_query_time).total_seconds()
            < self.zombie_query_interval_secs
        ):
            return
        self._last_zombie_query_time = timezone.utcnow()
        self.log.debug("Checking on possible zombie tasks")
        zombies = self._find_task_instance_zombies(dag, execution_date, session=session)
        if not zombies:
            return

        self.log.info("Found %s zombie tasks.", len(zombies))
        self._kill_zombies(dag, zombies=zombies, session=session)

    def _kill_zombies(self, dag, zombies, session):
        """
        copy paste from airflow.models.dagbag.DagBag.kill_zombies
        """
        for zombie in zombies:
            if zombie.task_id not in dag.task_ids:
                continue  # old implementation, can't happen in SingleJobRun

            self.k8s_executor.clear_zombie_task_instance(zombie_task_instance=zombie)
            # original zombie implementation,
            # we just call zombie handling at k8s scheduler
            #
            # task = dag.get_task(zombie.task_id)
            # ti = TaskInstance(task, zombie.execution_date)
            # # Get properties needed for failure handling from SimpleTaskInstance.
            # ti.start_date = zombie.start_date
            # ti.end_date = zombie.end_date
            # ti.try_number = zombie.try_number
            # ti.state = zombie.state
            # ti.test_mode = self.UNIT_TEST_MODE

        session.commit()

    @provide_session
    def _find_task_instance_zombies(self, dag, execution_date, session):
        """
        Find zombie task instances, which are tasks haven't heartbeated for too long
        and update the current zombie list.

        copy paste from DagFileProcessorAgent
        """
        now = timezone.utcnow()
        zombies = []

        # to avoid circular imports
        from airflow.jobs import LocalTaskJob as LJ

        TI = airflow.models.TaskInstance
        limit_dttm = timezone.utcnow() - datetime.timedelta(
            seconds=self.zombie_threshold_secs
        )

        tis = (
            session.query(TI)
            .join(LJ, TI.job_id == LJ.id)
            .filter(TI.state == State.RUNNING)
            .filter(TI.dag_id == dag.dag_id)
            .filter(TI.execution_date == execution_date)
            .filter(or_(LJ.state != State.RUNNING, LJ.latest_heartbeat < limit_dttm,))
            .all()
        )
        if tis:
            self.log.info("Failing jobs without heartbeat after %s", limit_dttm)
        for ti in tis:
            # sti = SimpleTaskInstance(ti)
            sti = ti
            self.log.info(
                "Detected zombie task instance: dag_id=%s task_id=%s execution_date= %s, try_number=%s",
                sti.dag_id,
                sti.task_id,
                sti.execution_date.isoformat(),
                sti.try_number,
            )
            zombies.append(ti)

        return zombies
