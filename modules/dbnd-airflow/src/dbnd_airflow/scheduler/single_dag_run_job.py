from __future__ import absolute_import, division, print_function, unicode_literals

import datetime
import logging

from airflow import executors, models
from airflow.jobs import BackfillJob, BaseJob
from airflow.models import DagRun, TaskInstance as TI
from airflow.ti_deps.dep_context import RUNNABLE_STATES, RUNNING_DEPS, DepContext
from airflow.utils import timezone
from airflow.utils.configuration import tmp_configuration_copy
from airflow.utils.db import provide_session
from airflow.utils.state import State
from sqlalchemy import and_, or_
from sqlalchemy.orm.session import make_transient

from databand import dbnd_config
from dbnd._core import current
from dbnd._core.constants import TaskRunState
from dbnd._core.current import get_databand_run
from dbnd._core.errors import DatabandSystemError, friendly_error
from dbnd._core.errors.base import DatabandFailFastError, DatabandRunError
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.utils.basics.singleton_context import SingletonContext
from dbnd_airflow.config import AirflowConfig
from dbnd_airflow.dbnd_task_executor.task_instance_state_manager import (
    AirflowTaskInstanceStateManager,
)


logger = logging.getLogger(__name__)

SCHEDUALED_OR_RUNNABLE = RUNNABLE_STATES.union({State.SCHEDULED})
END_STATES = (State.SUCCESS, State.FAILED, State.UPSTREAM_FAILED)


def _kill_zombies(dag_run, session):
    """
    Remove zombies DagRuns and TaskInstances.
    If the job ended but the DagRun is still not finished then
    fail the DR and related unfinished TIs
    """
    logger.info(
        "Job has ended but the related %s is still in state %s - marking it as failed",
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


# based on airflow BackfillJob
class SingleDagRunJob(BaseJob, SingletonContext):
    """
    A backfill job consists of a dag or subdag for a specific time range. It
    triggers a set of task instance runs, in the right order and lasts for
    as long as it takes for the set of task instance to be completed.
    """

    # ID_PREFIX should be based on BackfillJob
    # there is a lot of checks that are "not scheduled_job"
    # they uses run_name string
    ID_PREFIX = BackfillJob.ID_PREFIX + "manual_    "
    ID_FORMAT_PREFIX = ID_PREFIX + "{0}"

    # if we use real name of the class we need to load it at Airflow Webserver
    __mapper_args__ = {"polymorphic_identity": "BackfillJob"}

    def __init__(
        self,
        dag,
        execution_date,
        mark_success=False,
        donot_pickle=False,
        ignore_first_depends_on_past=False,
        ignore_task_deps=False,
        fail_fast=True,
        pool=None,
        delay_on_limit_secs=1.0,
        verbose=False,
        airflow_config=None,
        *args,
        **kwargs
    ):
        self.dag = dag
        self.dag_id = dag.dag_id
        self.execution_date = execution_date
        self.mark_success = mark_success
        self.donot_pickle = donot_pickle
        self.ignore_first_depends_on_past = ignore_first_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.fail_fast = fail_fast
        self.pool = pool
        self.delay_on_limit_secs = delay_on_limit_secs
        self.verbose = verbose

        self.terminating = False

        self._logged_count = 0  # counter for status update
        self._logged_status = ""  # last printed status

        self.ti_state_manager = AirflowTaskInstanceStateManager()
        self.airflow_config = airflow_config  # type: AirflowConfig
        super(SingleDagRunJob, self).__init__(*args, **kwargs)

    @property
    def _optimize(self):
        return self.airflow_config.optimize_airflow_db_access

    def _update_counters(self, ti_status, waiting_for_executor_result):
        """
        Updates the counters per state of the tasks that were running. Can re-add
        to tasks to run in case required.
        :param ti_status: the internal status of the backfill job tasks
        :type ti_status: DagRunJob._DagRunTaskStatus
        """
        for key, ti in list(ti_status.running.items()):
            # updated by StateManager
            if not self._optimize:
                ti.refresh_from_db()

            if ti.state == State.SUCCESS:
                ti_status.succeeded.add(key)
                self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                ti_status.running.pop(key)
                continue
            elif ti.state == State.SKIPPED:
                ti_status.skipped.add(key)
                self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                ti_status.running.pop(key)
                continue
            elif ti.state == State.FAILED:
                self.log.error("Task instance %s failed", ti)
                ti_status.failed.add(key)
                ti_status.running.pop(key)
                continue
            # special case: if the task needs to run again put it back.
            #
            # if we don't wait for the executor response then there's a race condition where the task gets rerun
            # before we process the response for the failure. The response handler then fails the task because it thinks
            # there's a mismatch between the task's state and the executor's result (which it gives priority).
            # This causes the task to be put in the to_run map again but if the task (which is already running) just
            # changes it's state to running it will cause the scheduler to put into the not_ready map by default
            # and then we can get a false positive deadlock error
            elif (
                ti.state == State.UP_FOR_RETRY
                and key not in waiting_for_executor_result
            ):
                self.log.warning("Task instance %s is up for retry", ti)
                ti_status.running.pop(key)
                ti_status.to_run[key] = ti
            # special case: The state of the task can be set to NONE by the task itself
            # when it reaches concurrency limits. It could also happen when the state
            # is changed externally, e.g. by clearing tasks from the ui. We need to cover
            # for that as otherwise those tasks would fall outside of the scope of
            # the backfill suddenly.
            elif ti.state == State.NONE:
                self.log.warning(
                    "FIXME: task instance %s state was set to none externally or "
                    "reaching concurrency limits. Re-adding task to queue.",
                    ti,
                )
                ti.set_state(State.SCHEDULED)
                ti_status.running.pop(key)
                ti_status.to_run[key] = ti

    def _manage_executor_state(self, running, waiting_for_executor_result):
        """
        Checks if the executor agrees with the state of task instances
        that are running
        :param running: dict of key, task to verify
        """
        executor = self.executor

        for key, state in list(executor.get_event_buffer().items()):
            # the fourth slot in the key is the try number (defined in TaskInstance.key). The keys in the scheduler maps are
            # determined at the start of the run and never updated - so the try number is stuck on 1. The executor however
            # returns the status for the current retry number so after the first try we ignore events from the executor unless
            # we fix the key
            key_as_list = list(key)
            key_as_list[3] = 1
            key = tuple(list(key_as_list))

            if key not in running:
                self.log.debug(
                    "Received executor state '%s' for Task %s not in running=%s",
                    state,
                    key,
                    running.values(),
                )
                continue

            if key not in waiting_for_executor_result:
                self.log.debug(
                    "Received executor state '%s' for Task %s not in waiting_for_executor_result=%s",
                    state,
                    key,
                    waiting_for_executor_result.values(),
                )
                continue

            waiting_for_executor_result.pop(key)

            ti = running[key]
            # updated by StateManager
            if not self._optimize or ti.state in [State.RUNNING, State.QUEUED]:
                # we refresh if we are not optimized
                # or we have ti in running state, there could be a racing between
                # our state manager sync and executor.get_event_buffer()
                # task could be Running, but while running get_event_buffer it can become SUCCESS
                ti.refresh_from_db()

            self.log.debug("Executor state: %s task %s", state, ti)

            if state == State.FAILED or state == State.SUCCESS:
                if ti.state == State.RUNNING or ti.state == State.QUEUED:
                    msg = (
                        "Executor reports task instance {} finished ({}) "
                        "although the task says its {}. Was the task "
                        "killed externally?".format(ti, state, ti.state)
                    )
                    self.log.error(msg)
                    ti.handle_failure(msg)

            # this is the case when internal tasks in executor were marked as "UPSTREAM_FAILED" (fail fast mode)
            # let just remove them from running.
            if state == State.UPSTREAM_FAILED:
                running.pop(key)

    @provide_session
    def _get_dag_run(self, session=None):
        """
        Returns a dag run for the given run date, which will be matched to an existing
        dag run if available or create a new dag run otherwise. If the max_active_runs
        limit is reached, this function will return None.
        :param run_date: the execution date for the dag run
        :type run_date: datetime
        :param session: the database session object
        :type session: Session
        :return: a DagRun in state RUNNING or None
        """

        # check if we are scheduling on top of a already existing dag_run
        # we could find a "scheduled" run instead of a "backfill"
        dagrun = DagRun.find(
            dag_id=self.dag.dag_id, execution_date=self.execution_date, session=session
        )
        if not dagrun:
            raise Exception("There is no dagrun in db!")
        dagrun = dagrun[0]
        # set required transient field
        dagrun.dag = self.dag

        # explicitly mark as backfill and running
        dagrun.state = State.RUNNING
        dagrun.verify_integrity(session=session)
        return dagrun

    @provide_session
    def _task_instances_for_dag_run(self, dag_run, session=None):
        """
        Returns a map of task instance key to task instance object for the tasks to
        run in the given dag run.
        :param dag_run: the dag run to get the tasks from
        :type dag_run: models.DagRun
        :param session: the database session object
        :type session: Session
        """
        tasks_to_run = {}

        if dag_run is None:
            return tasks_to_run

        # check if we have orphaned tasks
        self.reset_state_for_orphaned_tasks(filter_by_dag_run=dag_run, session=session)

        # for some reason if we don't refresh the reference to run is lost
        dag_run.refresh_from_db()
        make_transient(dag_run)

        # DBNDPATCH
        # implements batch update
        session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date,
            TI.state == State.NONE,
        ).update(
            {
                TI.state: State.SCHEDULED,
                TI.start_date: timezone.utcnow(),
                TI.end_date: timezone.utcnow(),
            }
        )
        # TODO(edgarRd): AIRFLOW-1464 change to batch query to improve perf
        #
        task_instances = dag_run.get_task_instances()

        for ti in task_instances:
            # all tasks part of the backfill are scheduled to run
            if ti.state == State.NONE:
                # no waiting for the airflow - batch upate
                # ti.set_state(State.SCHEDULED, session=session)

                ti.state = State.SCHEDULED
                ti.start_date = timezone.utcnow()
                ti.end_date = timezone.utcnow()
                session.merge(ti)
            if ti.state != State.REMOVED:
                ti._log = logging.getLogger("airflow.task")
                tasks_to_run[ti.key] = ti

        session.commit()
        return tasks_to_run

    def _log_progress(self, ti_status):

        msg = " | ".join(
            [
                "[progress]",
                "finished run {0} of {1}",
                "tasks waiting: {2}",
                "succeeded: {3}",
                "running: {4}",
                "failed: {5}",
                "skipped: {6}",
                "deadlocked: {7}",
                "not ready: {8}",
            ]
        ).format(
            ti_status.finished_runs,
            ti_status.total_runs,
            len(ti_status.to_run),
            len(ti_status.succeeded),
            len(ti_status.running),
            len(ti_status.failed),
            len(ti_status.skipped),
            len(ti_status.deadlocked),
            len(ti_status.not_ready),
        )

        self._logged_count += 1
        if self._logged_status != msg or self._logged_count > 30:
            self._logged_status = msg
            self._logged_count = 0
            self.log.info(msg)

        self.log.debug(
            "Finished dag run loop iteration. Remaining tasks %s",
            ti_status.to_run.values(),
        )

    @provide_session
    def _process_dag_task_instances(self, ti_status, executor, pickle_id, session=None):
        """
        Process a set of task instances from a set of dag runs. Special handling is done
        to account for different task instance states that could be present when running
        them in a backfill process.
        :param ti_status: the internal status of the job
        :type ti_status: DagRunJob._DagRunTaskStatus
        :param executor: the executor to run the task instances
        :type executor: BaseExecutor
        :param pickle_id: the pickle_id if dag is pickled, None otherwise
        :type pickle_id: int
        :param start_date: the start date of the backfill job
        :type start_date: datetime
        :param session: the current session object
        :type session: Session
        :return: the list of execution_dates for the finished dag runs
        :rtype: list
        """

        executed_run_dates = []

        # values() returns a view so we copy to maintain a full list of the TIs to run
        all_ti = list(ti_status.to_run.values())
        waiting_for_executor_result = {}

        while (len(ti_status.to_run) > 0 or len(ti_status.running) > 0) and len(
            ti_status.deadlocked
        ) == 0:
            if current.is_killed():
                raise friendly_error.task_execution.databand_context_killed(
                    "SingleDagRunJob scheduling main loop"
                )
            self.log.debug("*** Clearing out not_ready list ***")
            ti_status.not_ready.clear()

            self.ti_state_manager.refresh_task_instances_state(
                all_ti, self.dag.dag_id, self.execution_date, session=session
            )

            # we need to execute the tasks bottom to top
            # or leaf to root, as otherwise tasks might be
            # determined deadlocked while they are actually
            # waiting for their upstream to finish
            for task in self.dag.topological_sort():

                # TODO: too complicated mechanism,
                # it's not possible that we have multiple tasks with the same id in to run
                for key, ti in list(ti_status.to_run.items()):
                    if task.task_id != ti.task_id:
                        continue

                    if not self._optimize:
                        ti.refresh_from_db()

                    task = self.dag.get_task(ti.task_id)
                    ti.task = task

                    # TODO : do we need that?
                    # ignore_depends_on_past = (
                    #     self.ignore_first_depends_on_past and
                    #     ti.execution_date == (start_date or ti.start_date))
                    ignore_depends_on_past = False
                    self.log.debug("Task instance to run %s state %s", ti, ti.state)

                    # guard against externally modified tasks instances or
                    # in case max concurrency has been reached at task runtime
                    if ti.state == State.NONE:
                        self.log.warning(
                            "FIXME: task instance {} state was set to None "
                            "externally. This should not happen"
                        )
                        ti.set_state(State.SCHEDULED, session=session)

                    # The task was already marked successful or skipped by a
                    # different Job. Don't rerun it.
                    if ti.state == State.SUCCESS:
                        ti_status.succeeded.add(key)
                        self.log.debug("Task instance %s succeeded. Don't rerun.", ti)
                        ti_status.to_run.pop(key)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        continue
                    elif ti.state == State.SKIPPED:
                        ti_status.skipped.add(key)
                        self.log.debug("Task instance %s skipped. Don't rerun.", ti)
                        ti_status.to_run.pop(key)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        continue
                    elif ti.state == State.FAILED:
                        self.log.error("Task instance %s failed", ti)
                        ti_status.failed.add(key)
                        ti_status.to_run.pop(key)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        continue
                    elif ti.state == State.UPSTREAM_FAILED:
                        self.log.error("Task instance %s upstream failed", ti)
                        ti_status.failed.add(key)
                        ti_status.to_run.pop(key)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        continue

                    runtime_deps = []
                    if self.airflow_config.disable_dag_concurrency_rules:
                        # RUN Deps validate dag and task concurrency
                        # It's less relevant when we run in stand along mode with SingleDagRunJob
                        # from airflow.ti_deps.deps.runnable_exec_date_dep import RunnableExecDateDep
                        from airflow.ti_deps.deps.valid_state_dep import ValidStateDep

                        # from airflow.ti_deps.deps.dag_ti_slots_available_dep import DagTISlotsAvailableDep
                        # from airflow.ti_deps.deps.task_concurrency_dep import TaskConcurrencyDep
                        # from airflow.ti_deps.deps.pool_slots_available_dep import PoolSlotsAvailableDep
                        runtime_deps = {
                            # RunnableExecDateDep(),
                            ValidStateDep(SCHEDUALED_OR_RUNNABLE),
                            # DagTISlotsAvailableDep(),
                            # TaskConcurrencyDep(),
                            # PoolSlotsAvailableDep(),
                        }
                    else:
                        runtime_deps = RUNNING_DEPS

                    dagrun_dep_context = DepContext(
                        deps=runtime_deps,
                        ignore_depends_on_past=ignore_depends_on_past,
                        ignore_task_deps=self.ignore_task_deps,
                        flag_upstream_failed=True,
                    )

                    # Is the task runnable? -- then run it
                    # the dependency checker can change states of tis
                    if ti.are_dependencies_met(
                        dep_context=dagrun_dep_context,
                        session=session,
                        verbose=self.verbose,
                    ):
                        ti.refresh_from_db(lock_for_update=True, session=session)
                        if (
                            ti.state == State.SCHEDULED
                            or ti.state == State.UP_FOR_RETRY
                        ):
                            if executor.has_task(ti):
                                self.log.debug(
                                    "Task Instance %s already in executor "
                                    "waiting for queue to clear",
                                    ti,
                                )
                            else:
                                self.log.debug("Sending %s to executor", ti)
                                # if ti.state == State.UP_FOR_RETRY:
                                #     ti._try_number += 1
                                # Skip scheduled state, we are executing immediately
                                ti.state = State.QUEUED
                                session.merge(ti)

                                cfg_path = None
                                if executor.__class__ in (
                                    executors.LocalExecutor,
                                    executors.SequentialExecutor,
                                ):
                                    cfg_path = tmp_configuration_copy()

                                executor.queue_task_instance(
                                    ti,
                                    mark_success=self.mark_success,
                                    pickle_id=pickle_id,
                                    ignore_task_deps=self.ignore_task_deps,
                                    ignore_depends_on_past=ignore_depends_on_past,
                                    pool=self.pool,
                                    cfg_path=cfg_path,
                                )

                                ti_status.to_run.pop(key)
                                ti_status.running[key] = ti
                                waiting_for_executor_result[key] = ti
                        session.commit()
                        continue

                    if ti.state == State.UPSTREAM_FAILED:
                        self.log.error("Task instance %s upstream failed", ti)
                        ti_status.failed.add(key)
                        ti_status.to_run.pop(key)
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        continue

                    # special case
                    if ti.state == State.UP_FOR_RETRY:
                        self.log.debug(
                            "Task instance %s retry period not " "expired yet", ti
                        )
                        if key in ti_status.running:
                            ti_status.running.pop(key)
                        ti_status.to_run[key] = ti
                        continue

                    # all remaining tasks
                    self.log.debug("Adding %s to not_ready", ti)
                    ti_status.not_ready.add(key)
            # execute the tasks in the queue
            self.heartbeat()
            executor.heartbeat()

            # If the set of tasks that aren't ready ever equals the set of
            # tasks to run and there are no running tasks then the backfill
            # is deadlocked
            if (
                ti_status.not_ready
                and ti_status.not_ready == set(ti_status.to_run)
                and len(ti_status.running) == 0
            ):
                self.log.warning(
                    "Deadlock discovered for ti_status.to_run=%s",
                    ti_status.to_run.values(),
                )
                ti_status.deadlocked.update(ti_status.to_run.values())
                ti_status.to_run.clear()

            self.ti_state_manager.refresh_task_instances_state(
                all_ti, self.dag.dag_id, self.execution_date, session=session
            )

            # check executor state
            self._manage_executor_state(ti_status.running, waiting_for_executor_result)

            # update the task counters
            self._update_counters(ti_status, waiting_for_executor_result)

            # update dag run state
            _dag_runs = ti_status.active_runs[:]
            for run in _dag_runs:
                run.update_state(session=session)

                self._update_databand_task_run_states(run)

                if run.state in State.finished():
                    ti_status.finished_runs += 1
                    ti_status.active_runs.remove(run)
                    executed_run_dates.append(run.execution_date)

            self._log_progress(ti_status)

            if self.fail_fast and ti_status.failed:
                logger.error(
                    "terminating executor because a task failed and fail_fast mode is enabled"
                )
                raise DatabandFailFastError(
                    "Failing whole pipeline as it has failed/canceled tasks %s"
                    % [t for t in ti_status.failed],
                )

        # return updated status
        return executed_run_dates

    def _update_databand_task_run_states(self, run):
        """
        Sync states between DBND and Airflow
        we need to sync state into Tracker,
        if we use "remote" executors (parallel/k8s) we need to copy state into
        current process (scheduler)
        """

        # this is the only state we want to propogate into Databand
        # all other state changes are managed by databand itself by it's own state machine
        dr = get_databand_run()

        task_runs = []

        # sync all states
        for ti in run.get_task_instances():
            task_run = dr.get_task_run_by_af_id(ti.task_id)  # type: TaskRun
            if not task_run:
                continue

            # UPSTREAM FAILED tasks are not going to "run" , so no code will update their state
            if (
                ti.state == State.UPSTREAM_FAILED
                and task_run.task_run_state != TaskRunState.UPSTREAM_FAILED
            ):
                task_run.set_task_run_state(TaskRunState.UPSTREAM_FAILED, track=False)
                task_runs.append(task_run)
            # update only in memory state
            if (
                ti.state == State.SUCCESS
                and task_run.task_run_state != TaskRunState.SUCCESS
            ):
                task_run.set_task_run_state(TaskRunState.SUCCESS, track=False)
            if (
                ti.state == State.FAILED
                and task_run.task_run_state != TaskRunState.FAILED
            ):
                task_run.set_task_run_state(TaskRunState.FAILED, track=False)

        # optimization to write all updates in batch
        if task_runs:
            dr.tracker.set_task_run_states(task_runs)

    @provide_session
    def _collect_errors(self, ti_status, session=None):
        err = ""
        if ti_status.failed:
            dr = get_databand_run()
            upstream_failed = []
            failed = []
            for fail_info in ti_status.failed:
                airflow_task_id = fail_info[1]
                task_run = dr.get_task_run(airflow_task_id)
                task_name = task_run.task.task_name
                if task_run.task_run_state == State.UPSTREAM_FAILED:
                    # we don't want to show upstream failed in the list
                    upstream_failed.append(task_name)
                else:
                    failed.append(task_name)
            if upstream_failed:
                err += (
                    "Task that didn't run because "
                    "of failed dependency:\n\t{}\n".format("\n\t".join(upstream_failed))
                )
            if failed:
                err += "Failed tasks are:\n\t{}".format("\n\t".join(failed))
        if ti_status.deadlocked:
            err += (
                "---------------------------------------------------\n"
                "DagRunJob is deadlocked."
            )
            deadlocked_depends_on_past = any(
                t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=False),
                    session=session,
                    verbose=self.verbose,
                )
                != t.are_dependencies_met(
                    dep_context=DepContext(ignore_depends_on_past=True),
                    session=session,
                    verbose=self.verbose,
                )
                for t in ti_status.deadlocked
            )
            if deadlocked_depends_on_past:
                err += (
                    "Some of the deadlocked tasks were unable to run because "
                    'of "depends_on_past" relationships. Try running the '
                    "backfill with the option "
                    '"ignore_first_depends_on_past=True" or passing "-I" at '
                    "the command line."
                )
            err += " These tasks have succeeded:\n{}\n".format(ti_status.succeeded)
            err += " These tasks are running:\n{}\n".format(ti_status.running)
            err += " These tasks have failed:\n{}\n".format(ti_status.failed)
            err += " These tasks are skipped:\n{}\n".format(ti_status.skipped)
            err += " These tasks are deadlocked:\n{}\n".format(ti_status.deadlocked)

        return err

    @provide_session
    def _execute(self, session=None):
        """
        Initializes all components required to run a dag for a specified date range and
        calls helper method to execute the tasks.
        """
        # Trigger cleaning
        if self.airflow_config.clean_zombies_during_backfill:
            ClearZombieJob().run()

        ti_status = BackfillJob._DagRunTaskStatus()

        # picklin'
        pickle_id = self.dag.pickle_id
        # We don't need to pickle our dag again as it already pickled on job creattion
        # also this will save it into databand table, that have no use for the airflow
        # if not self.donot_pickle and self.executor.__class__ not in (
        #     executors.LocalExecutor,
        #     executors.SequentialExecutor,
        # ):
        #     pickle_id = airflow_pickle(self.dag, session=session)

        executor = self.executor
        executor.start()

        ti_status.total_runs = 1  # total dag runs in backfill

        dag_run = None
        try:
            dag_run = self._get_dag_run(session=session)

            # Create relation DagRun <> Job
            dag_run.conf = {"job_id": self.id}
            session.merge(dag_run)
            session.commit()

            run_date = dag_run.execution_date
            if dag_run is None:
                raise DatabandSystemError("Can't build dagrun")

            tis_map = self._task_instances_for_dag_run(dag_run, session=session)

            if not tis_map:
                raise DatabandSystemError("There are no task instances to run!")
            ti_status.active_runs.append(dag_run)
            ti_status.to_run.update(tis_map or {})

            processed_dag_run_dates = self._process_dag_task_instances(
                ti_status=ti_status,
                executor=executor,
                pickle_id=pickle_id,
                session=session,
            )
            ti_status.executed_dag_run_dates.update(processed_dag_run_dates)

            err = self._collect_errors(ti_status=ti_status, session=session)
            if err:
                raise DatabandRunError("Airflow executor has failed to run the run")

            if run_date not in ti_status.executed_dag_run_dates:
                self.log.warning(
                    "Dag %s is not marked as completed!  %s not found in %s",
                    self.dag_id,
                    run_date,
                    ti_status.executed_dag_run_dates,
                )
        finally:
            # in sequential executor a keyboard interrupt would reach here and
            # then executor.end() -> heartbeat() -> sync() will cause the queued commands
            # to be run again before exiting
            if hasattr(executor, "commands_to_run"):
                executor.commands_to_run = []
            try:
                executor.end()
            except Exception:
                logger.exception("Failed to terminate executor")
            if dag_run and dag_run.state == State.RUNNING:
                _kill_zombies(dag_run, session)
            session.commit()

        self.log.info("Run is completed. Exiting.")

    @provide_session
    def heartbeat_callback(self, session=None):
        """Self destruct task if state has been moved away from running externally"""

        if self.terminating:
            # ensure termination if processes are created later
            # self.executor.terminate()
            return
        self.terminating = True


@provide_session
def find_and_kill_zombies(args, session=None):
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
        _kill_zombies(dag_run=dr, session=session)

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
            find_and_kill_zombies(None, session=session)
        except Exception as err:
            # Fail silently as this is not crucial process
            session.rollback()
            logging.warning("Cleaning zombies failed: %s", err)
