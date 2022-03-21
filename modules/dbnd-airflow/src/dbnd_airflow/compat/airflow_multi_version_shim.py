from airflow import models
from airflow.configuration import conf as airflow_conf
from airflow.jobs.base_job import BaseJob
from airflow.utils import helpers
from airflow.utils.db import provide_session
from airflow.utils.state import State
from sqlalchemy import and_, or_

from dbnd_airflow.constants import AIRFLOW_VERSION_2


if AIRFLOW_VERSION_2:
    from airflow.kubernetes.secret import Secret  # noqa: F401
    from airflow.models.taskinstance import SimpleTaskInstance  # noqa: F401
    from airflow.utils.log.logging_mixin import LoggingMixin  # noqa: F401
else:
    from airflow import LoggingMixin  # noqa: F401
    from airflow.contrib.kubernetes.secret import Secret  # noqa: F401
    from airflow.utils.dag_processing import SimpleTaskInstance  # noqa: F401


def get_airflow_conf_log_folder() -> str:
    if AIRFLOW_VERSION_2:
        return airflow_conf.get("logging", "BASE_LOG_FOLDER")

    return airflow_conf.get("core", "BASE_LOG_FOLDER")


def get_airflow_conf_remote_logging() -> bool:
    if AIRFLOW_VERSION_2:
        return airflow_conf.getboolean("logging", "remote_logging")
    else:
        return airflow_conf.getboolean("core", "remote_logging")


def is_task_instance_finished(current_state: str) -> bool:
    # in AF 1.x this is a function returning array,
    #   while in 2.x - class variable
    if AIRFLOW_VERSION_2:
        return current_state in State.finished
    else:
        return current_state in State.finished()


if AIRFLOW_VERSION_2:
    from airflow.executors.local_executor import LocalExecutor  # noqa: F401
    from airflow.executors.sequential_executor import SequentialExecutor  # noqa: F401
else:
    from airflow.executors import LocalExecutor, SequentialExecutor  # noqa: F401


@provide_session
def reset_state_for_orphaned_tasks(
    single_dag_run_job: BaseJob, filter_by_dag_run=None, session=None
):
    """
    It was removed from Airflow 2.x Worth looking why it happened in Airflow github
    For now just replicated behaviour of v1. Probably not needed anymore

    This function checks if there are any tasks in the dagrun (or all)
    that have a scheduled state but are not known by the
    executor. If it finds those it will reset the state to None
    so they will get picked up again.
    The batch option is for performance reasons as the queries are made in
    sequence.

    :param filter_by_dag_run: the dag_run we want to process, None if all
    :type filter_by_dag_run: airflow.models.DagRun
    :return: the TIs reset (in expired SQLAlchemy state)
    :rtype: list[airflow.models.TaskInstance]
    """
    from airflow.jobs.backfill_job import BackfillJob

    queued_tis = single_dag_run_job.executor.queued_tasks
    # also consider running as the state might not have changed in the db yet
    running_tis = single_dag_run_job.executor.running

    resettable_states = [State.SCHEDULED, State.QUEUED]
    TI = models.TaskInstance
    DR = models.DagRun
    if filter_by_dag_run is None:
        resettable_tis = (
            session.query(TI)
            .join(
                DR, and_(TI.dag_id == DR.dag_id, TI.execution_date == DR.execution_date)
            )
            .filter(
                DR.state == State.RUNNING,
                DR.run_id.notlike(BackfillJob.ID_PREFIX + "%"),
                TI.state.in_(resettable_states),
            )
        ).all()
    else:
        resettable_tis = filter_by_dag_run.get_task_instances(
            state=resettable_states, session=session
        )
    tis_to_reset = []
    # Can't use an update here since it doesn't support joins
    for ti in resettable_tis:
        if ti.key not in queued_tis and ti.key not in running_tis:
            tis_to_reset.append(ti)

    if len(tis_to_reset) == 0:
        return []

    def query(result, items):
        filter_for_tis = [
            and_(
                TI.dag_id == ti.dag_id,
                TI.task_id == ti.task_id,
                TI.execution_date == ti.execution_date,
            )
            for ti in items
        ]
        reset_tis = (
            session.query(TI)
            .filter(or_(*filter_for_tis), TI.state.in_(resettable_states))
            .with_for_update()
            .all()
        )
        for ti in reset_tis:
            ti.state = State.NONE
            session.merge(ti)
        return result + reset_tis

    reset_tis = helpers.reduce_in_chunks(
        query, tis_to_reset, [], single_dag_run_job.max_tis_per_query
    )

    task_instance_str = "\n\t".join([repr(x) for x in reset_tis])
    session.commit()

    single_dag_run_job.log.info(
        "Reset the following %s TaskInstances:\n\t%s", len(reset_tis), task_instance_str
    )
    return reset_tis
