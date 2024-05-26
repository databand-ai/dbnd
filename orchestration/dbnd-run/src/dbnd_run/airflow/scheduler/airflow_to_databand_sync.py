# © Copyright Databand.ai, an IBM Company 2022

from __future__ import absolute_import, division, print_function, unicode_literals

import datetime
import logging
import typing

from airflow.utils.state import State

from dbnd._core.constants import TaskRunState, UpdateSource
from dbnd._core.current import get_databand_run
from dbnd._core.task_run.task_run import TaskRun
from dbnd_run.airflow.compat import AIRFLOW_VERSION_2
from dbnd_run.airflow.compat.airflow_multi_version_shim import is_task_instance_finished
from dbnd_run.airflow.config import AirflowConfig
from dbnd_run.current import get_run_executor


if AIRFLOW_VERSION_2:
    from airflow.ti_deps.dependencies_states import RUNNABLE_STATES
else:
    from airflow.ti_deps.dep_context import RUNNABLE_STATES

if typing.TYPE_CHECKING:
    from airflow.models import TaskInstance

logger = logging.getLogger(__name__)

SCHEDULED_OR_RUNNABLE = RUNNABLE_STATES.union({State.SCHEDULED})


def sync_task_run_attempts_retries(ti_status):
    databand_run = get_databand_run()
    for dag_run in ti_status.active_runs:
        # there is only one dag run, however, it's easier to iterate this way
        for ti in dag_run.get_task_instances():
            task_run = databand_run.get_task_run_by_af_id(ti.task_id)  # type: TaskRun
            if not task_run:
                continue
            # looking for retry tasks
            af_task_try_number = _get_af_task_try_number(ti)

            if task_run.attempt_number < af_task_try_number:
                logger.info(
                    "Found a new attempt for task %60s (dbnd=%s - af=%s) in Airflow DB(might come from Pod/Scheduler). Submitting to Databand.",
                    ti.task_id,
                    task_run.attempt_number,
                    af_task_try_number,
                )
                # update in memory object with new attempt number
                from dbnd_run.task_ctrl.task_run_executor import TaskRunExecutor

                task_run.set_task_run_attempt(af_task_try_number)
                task_run.task_run_executor = TaskRunExecutor(
                    task_run,
                    run_executor=task_run.task_run_executor.run_executor,
                    task_engine=task_run.task_run_executor.task_engine,
                )

                # sync the tracker with the new task_run_attempt
                databand_run.tracker.tracking_store.add_task_runs(
                    run=databand_run, task_runs=[task_run]
                )
                report_airflow_task_instance(ti.dag_id, ti.execution_date, [task_run])
            elif task_run.attempt_number > af_task_try_number:
                logger.info(
                    "Found a task run in Airflow DB with AF attempt lower than DBND for task %60s (dbnd=%s - af=%s), skipping",
                    ti.task_id,
                    task_run.attempt_number,
                    af_task_try_number,
                )


def update_databand_task_run_states(dagrun):
    """
    Sync states between DBND and Airflow
    we need to sync state into Tracker,
    if we use "remote" executors (parallel/k8s) we need to copy state into
    current process (scheduler)
    """

    # this is the only state we want to propogate into Databand
    # all other state changes are managed by databand itself by it's own state machine
    run_executor = get_run_executor()
    databand_run = run_executor.run

    task_runs = []

    # sync all states

    # These tasks need special treatment because Airflow doesn't manage sub-pipelines
    #   for this, we need to process failures in child tasks first
    #   and decide if the parent sub-pipeline has failed
    upstream_failed_tasks: typing.List[TaskInstance] = []

    for ti in dagrun.get_task_instances():
        task_run = databand_run.get_task_run_by_af_id(ti.task_id)  # type: TaskRun
        if not task_run:
            continue

        # UPSTREAM FAILED tasks are not going to "run" , so no code will update their state
        if (
            ti.state == State.UPSTREAM_FAILED
            and task_run.task_run_state != TaskRunState.UPSTREAM_FAILED
        ):
            upstream_failed_tasks.append(ti)

        # update only in memory state
        if (
            ti.state == State.SUCCESS
            and task_run.task_run_state != TaskRunState.SUCCESS
        ):
            task_run.set_task_run_state(TaskRunState.SUCCESS, track=False)
        if ti.state == State.FAILED and task_run.task_run_state != TaskRunState.FAILED:
            task_run.set_task_run_state(TaskRunState.FAILED, track=False)

    # process them at the last step, when we have knowledge about the child tasks
    for ti in upstream_failed_tasks:
        task_run: TaskRun = databand_run.get_task_run_by_af_id(ti.task_id)

        state = run_executor.get_upstream_failed_task_run_state(task_run)
        logger.info("Setting %s to %s", task_run.task.task_id, state)
        task_run.set_task_run_state(state, track=False)
        task_runs.append(task_run)

    # optimization to write all updates in batch
    if task_runs:
        databand_run.tracker.set_task_run_states(task_runs)


def _get_af_task_try_number(af_task_instance):
    """
    Return the expected try_number from airflow's TaskInstance

    Airflow's TaskInstance have two attributes - `_try_number` and `try_number` here is the change in flow for those two:
    State                                   _try_number     try_number      expected
    ================================================================================
    pre-run (scheduled/submitted/queued)    0               1               1
    running                                 1               1               1
    finished (success/fail/...)             1               2               1
    pre-rerun (scheduled/submitted/queued)  1               2               2
    rerunning                               2               2               2
    finished (success/fail/...)             2               3               2
    """
    if is_task_instance_finished(af_task_instance.state):
        return af_task_instance.try_number - 1

    return af_task_instance.try_number


def report_airflow_task_instance(
    dag_id, execution_date, task_runs, airflow_config=None
):
    # type: (str, datetime, List[TaskRun], Optional[AirflowConfig]) ->None
    """
    report the relevant airflow task instances to the given task runs
    """
    from dbnd.api.tracking_api import AirflowTaskInfo

    af_instances = []
    for task_run in task_runs:
        if not task_run.is_reused:
            # we build airflow infos only for not reused tasks
            af_instance = AirflowTaskInfo(
                execution_date=execution_date,
                dag_id=dag_id,
                task_id=task_run.task_af_id,
                task_run_attempt_uid=task_run.task_run_attempt_uid,
            )
            af_instances.append(af_instance)

    if airflow_config is None:
        airflow_config = AirflowConfig.from_databand_context()

    if af_instances and airflow_config.webserver_url:
        first_task_run = task_runs[0]
        first_task_run.tracker.tracking_store.save_airflow_task_infos(
            airflow_task_infos=af_instances,
            source=UpdateSource.dbnd,
            base_url=airflow_config.webserver_url,
        )
