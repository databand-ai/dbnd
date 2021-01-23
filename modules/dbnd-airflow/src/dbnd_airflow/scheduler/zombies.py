import datetime
import logging

import airflow

from airflow import LoggingMixin
from airflow.configuration import conf
from airflow.jobs import BackfillJob, BaseJob
from airflow.models import DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.state import State
from sqlalchemy import and_, or_

from dbnd._core.log.logging_utils import PrefixLoggerAdapter


logger = logging.getLogger(__name__)

END_STATES = (State.SUCCESS, State.FAILED, State.UPSTREAM_FAILED)


def _kill_dag_run_zombi(dag_run, session):
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
        _kill_dag_run_zombi(dag_run=dr, session=session)

    logger.info("Cleaning done!")


class ClearZombieJob(BaseJob):
    ID_PREFIX = BackfillJob.ID_PREFIX + "manual_    "
    ID_FORMAT_PREFIX = ID_PREFIX + "{0}"
    __mapper_args__ = {"polymorphic_identity": "BackfillJob"}
    TYPE = "ZombieJob"
    TIME_BETWEEN_RUNS = 60 * 5

    def __init__(self, *args, **kwargs):
        super(ClearZombieJob, self).__init__(*args, **kwargs)
        # Trick for distinguishing this job from other backfills
        self.executor_class = self.TYPE

    @provide_session
    def _execute(self, session=None):
        last_run_date = (
            session.query(ClearZombieJob.latest_heartbeat)
            .filter(ClearZombieJob.executor_class == self.TYPE)
            .order_by(ClearZombieJob.end_date.asc())
            .first()
        )
        if (
            last_run_date
            and last_run_date[0] + datetime.timedelta(seconds=self.TIME_BETWEEN_RUNS)
            > timezone.utcnow()
        ):
            # If we cleaned zombies recently we can do nothing
            return
        try:
            find_and_kill_dagrun_zombies(None, session=session)
        except Exception as err:
            # Fail silently as this is not crucial process
            session.rollback()
            logging.warning("Cleaning zombies failed: %s", err)


class ClearZombieTaskInstancesForDagRun(LoggingMixin):
    """
    workaround for SingleDagRunJob to clean "missing" tasks
    """

    def __init__(self):
        super(ClearZombieTaskInstancesForDagRun, self).__init__()
        self.zombie_threshold_secs = conf.getint(
            "scheduler", "scheduler_zombie_task_threshold"
        )
        self.zombie_query_interval_secs = 60
        self._last_zombie_query_time = None
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
        from airflow.models.taskinstance import TaskInstance  # Avoid circular import

        for zombie in zombies:
            if zombie.task_id in dag.task_ids:
                task = dag.get_task(zombie.task_id)
                ti = TaskInstance(task, zombie.execution_date)
                # Get properties needed for failure handling from SimpleTaskInstance.
                ti.start_date = zombie.start_date
                ti.end_date = zombie.end_date
                ti.try_number = zombie.try_number
                ti.state = zombie.state
                # ti.test_mode = self.UNIT_TEST_MODE
                ti.handle_failure(
                    "{} detected as zombie".format(ti),
                    ti.test_mode,
                    ti.get_template_context(),
                )
                self.log.info("Marked zombie job %s as %s", ti, ti.state)
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
                "Detected zombie job with dag_id %s, task_id %s, and execution date %s",
                sti.dag_id,
                sti.task_id,
                sti.execution_date.isoformat(),
            )
            zombies.append(ti)

        return zombies
