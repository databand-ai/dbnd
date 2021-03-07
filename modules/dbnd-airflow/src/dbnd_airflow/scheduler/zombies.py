import datetime
import logging
import typing

import airflow

from airflow import LoggingMixin
from airflow.configuration import conf
from airflow.jobs import BaseJob
from airflow.models import DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.state import State
from sqlalchemy import and_, or_

from dbnd._core.log.logging_utils import PrefixLoggerAdapter


if typing.TYPE_CHECKING:
    from airflow.contrib.executors.kubernetes_executor import KubernetesExecutor

logger = logging.getLogger(__name__)

END_STATES = [State.SUCCESS, State.FAILED, State.UPSTREAM_FAILED]


@provide_session
def _kill_dag_run_zombies(dag_run, session=None):
    """
    Remove zombies DagRuns and TaskInstances.
    If the job ended but the DagRun is still not finished then
    fail the DR and related unfinished TIs
    """
    logger.info(
        "ZOMBIE: Job has ended but the related %s is still in state %s - marking it as failed",
        dag_run,
        dag_run.state,
    )
    qry = session.query(TI).filter(
        TI.dag_id == dag_run.dag_id,
        TI.execution_date == dag_run.execution_date,
        TI.state.notin_(END_STATES),
    )

    tis = qry.all()
    logger.info("Marking %s related TaskInstance as failed", len(tis))

    qry.update(
        {TI.state: State.FAILED, TI.end_date: timezone.utcnow()},
        synchronize_session=False,
    )
    dag_run.state = State.FAILED
    session.merge(dag_run)
    session.commit()


@provide_session
def find_and_kill_dagrun_zombies(args, session=None):
    """
    Find zombie DagRuns and TaskInstances that are in not end state and for which related
    BackfillJob has failed.
    """
    seconds_from_last_heartbeat = 60
    last_expected_heartbeat = timezone.utcnow() - datetime.timedelta(
        seconds=seconds_from_last_heartbeat
    )
    logger.info(
        "Cleaning zombie tasks with heartbeat older than: %s", last_expected_heartbeat,
    )

    # Select BackfillJob that failed or are still running
    # but have stalled heartbeat
    failed_jobs_id_qry = (
        session.query(BaseJob.id)
        .filter(BaseJob.job_type == "BackfillJob")
        .filter(
            or_(
                BaseJob.state == State.FAILED,
                and_(
                    BaseJob.state == State.RUNNING,
                    BaseJob.latest_heartbeat < last_expected_heartbeat,
                ),
            )
        )
        .all()
    )
    failed_jobs_id = set(r[0] for r in failed_jobs_id_qry)

    logger.info("Found %s stale jobs", len(failed_jobs_id))

    running_dag_runs = session.query(DagRun).filter(DagRun.state == State.RUNNING).all()

    for dr in running_dag_runs:
        if not isinstance(dr.conf, dict):
            continue

        job_id = dr.conf.get("job_id")
        if job_id not in failed_jobs_id:
            continue

        logger.info(
            "DagRun %s is in state %s but related job has failed. Cleaning zombies.",
            dr,
            dr.state,
        )
        # Now the DR is running but job running it has failed
        _kill_dag_run_zombies(dag_run=dr, session=session)

    logger.info("Cleaning done!")


class ClearZombieTaskInstancesForDagRun(LoggingMixin):
    """
    workaround for SingleDagRunJob to clean "missing" tasks
    for now it works for kubernetes only,
    as k8s executor requires "explicit" _clean_state for "zombie" tasks
    otherwise, they will not be submitted (as they are still at executior.running tasks
    """

    def __init__(self, k8s_executor):
        super(ClearZombieTaskInstancesForDagRun, self).__init__()
        self.zombie_threshold_secs = conf.getint(
            "scheduler", "scheduler_zombie_task_threshold"
        )
        self.zombie_query_interval_secs = 10
        self._last_zombie_query_time = None
        self.k8s_executor = k8s_executor  # type: KubernetesExecutor
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
