# © Copyright Databand.ai, an IBM Company 2022

import logging

from more_itertools import first

from dbnd._core.constants import RunState, TaskRunState
from dbnd._core.context.databand_context import DatabandContext
from dbnd._core.run.databand_run import DatabandRun, new_databand_run
from dbnd._core.run.run_banner import print_tasks_tree
from dbnd_luigi.luigi_task import wrap_luigi_task


logger = logging.getLogger(__name__)


class LuigiRunManager:
    def __init__(self):
        self._context_managers = []

        # context exists
        self._databand_context = None

        # run exists
        self._databand_run = None

        self.events_active = False
        self._current_execution_context = None
        self._driver_task_run = None
        self._root_dbnd_task = None
        self.task_cache = dict()
        self._missing_dep = set()
        self.active = False

    def _enter_databand_run(self, scheduled_tasks=None):
        self.init_context()

        if scheduled_tasks and not self.root_dbnd_task:
            # This code is reached only when an orphaned task is executed
            self.root_dbnd_task = self.get_dbnd_task(first(scheduled_tasks.values()))

        self._databand_run = run = self._enter_cm(
            new_databand_run(
                context=self._databand_context, job_name=self.root_dbnd_task.task_name
            )
        )  # type: DatabandRun

        # TODO: do we need that?
        raise Exception()
        # self._driver_task_run = run.build_and_set_driver_task_run(
        #     driver_task=Task(task_name=SystemTaskName.driver, task_is_system=True)
        # )

        self._driver_task_run.task.descendants.add_child(self.root_dbnd_task.task_id)

        # assign root task
        run.root_task = self.root_dbnd_task
        # Start the tracking process
        for task in run.root_task.task_dag.subdag_tasks():
            run._build_and_add_task_run(task)

        # we only update states not submitting data
        self._driver_task_run.set_task_run_state(
            state=TaskRunState.RUNNING, track=False
        )
        for task in self._missing_dep:
            task.current_task_run.set_task_run_state(
                state=TaskRunState.UPSTREAM_FAILED, track=False
            )
        run.tracker.init_run()

        self._enter_cm(self._driver_task_run.task_run_executor.task_run_track_execute())
        print_tasks_tree(run.root_task_run.task, run.task_runs)
        if not self.get_non_finished_sub_tasks():
            # we have no more tasks to run.. probably it's a failure
            self.finish_run(TaskRunState.FAILED)
            return

    def _enter_cm(self, cm):
        val = cm.__enter__()
        self._context_managers.append(cm)
        return val

    def _close_all_context_managers(self):
        while self._context_managers:
            cm = self._context_managers.pop()
            cm.__exit__(None, None, None)

    def init_databand_run(self, scheduled_tasks=None):
        if not self._databand_run:
            self._enter_databand_run(scheduled_tasks)

    @property
    def current_execution_context(self):
        return self._current_execution_context

    @current_execution_context.setter
    def current_execution_context(self, value):
        self._current_execution_context = value

    def add_missing_dep(self, missing_dep):
        self._missing_dep.add(missing_dep)

    @property
    def root_dbnd_task(self):
        return self._root_dbnd_task

    @root_dbnd_task.setter
    def root_dbnd_task(self, value):
        self._root_dbnd_task = value

    def get_dbnd_task(self, luigi_task):
        self.init_context()

        if luigi_task.task_id in self.task_cache:
            _, dbnd_task = self.task_cache[luigi_task.task_id]
        else:
            dbnd_task = wrap_luigi_task(luigi_task)
            self.task_cache[luigi_task.task_id] = (luigi_task, dbnd_task)

        return dbnd_task

    def init_context(self):
        if not self._databand_context:
            self._databand_context = DatabandContext(name="luigi")
            # Must enter context before creating dbnd task!
            dc = self._enter_cm(
                DatabandContext.context(_context=self._databand_context)
            )

    def encounter_task(self, dbnd_task):
        if not self.root_dbnd_task:
            self.root_dbnd_task = dbnd_task

    def start_task(self, dbnd_task):
        self.encounter_task(dbnd_task)

        self.current_execution_context = (
            dbnd_task.current_task_run.task_run_executor.task_run_track_execute()
        )

        self.current_execution_context.__enter__()
        dbnd_task.current_task_run.set_task_run_state(state=TaskRunState.RUNNING)

    def finish_task(self, dbnd_task, status, err=None):
        dbnd_task.current_task_run.set_task_run_state(state=status, error=err)

        if self.current_execution_context:
            self.current_execution_context.__exit__(None, None, None)

        if self.root_dbnd_task == dbnd_task:
            self.finish_run(status, error=err)

        elif not self.get_non_finished_sub_tasks():
            self.finish_run(TaskRunState.FAILED)

    def finish_run(self, task_run_state, error=None):
        self._driver_task_run.set_task_run_state(state=task_run_state, error=error)
        self._databand_run.set_run_state(
            RunState.FAILED
            if task_run_state != TaskRunState.SUCCESS
            else RunState.SUCCESS
        )
        self.stop_tracking()

    def stop_tracking(self):
        self._close_all_context_managers()

    def get_non_finished_sub_tasks(self):
        subdag = self.root_dbnd_task.task_dag.subdag_tasks()
        return set(
            task
            for task in subdag
            if not self.is_done(task) and task not in self._missing_dep
        )

    def is_done(self, task):
        return task._complete() or (
            task.current_task_run.task_run_state
            and task.current_task_run.task_run_state.final_states()
        )
