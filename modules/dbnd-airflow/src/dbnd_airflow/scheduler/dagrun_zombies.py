# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging
import typing

from airflow.models import DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.state import State
from sqlalchemy import and_, or_


try:
    from airflow.jobs import BaseJob
except ImportError:
    from airflow.jobs.base_job import BaseJob


if typing.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

END_STATES = [State.SUCCESS, State.FAILED, State.UPSTREAM_FAILED]


@provide_session
def fix_zombie_dagrun_task_instances(dag_run, session=None):
    """
    Remove zombies DagRuns and TaskInstances.
    If the job ended but the DagRun is still not finished then
    fail the DR and related unfinished TIs
    """

    qry = session.query(TI).filter(
        TI.dag_id == dag_run.dag_id,
        TI.execution_date == dag_run.execution_date,
        TI.state.notin_(END_STATES),
    )

    tis_count = qry.count()
    if not tis_count:
        return

    logger.info("Marking %s task instances at %s as failed", tis_count, dag_run)

    qry.update(
        {TI.state: State.FAILED, TI.end_date: timezone.utcnow()},
        synchronize_session=False,
    )


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
        "Cleaning zombie tasks with heartbeat older than: %s", last_expected_heartbeat
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
        fix_zombie_dagrun_task_instances(dag_run=dr, session=session)

        dr.state = State.FAILED
        session.merge(dr)
        session.commit()

    logger.info("Cleaning done!")
