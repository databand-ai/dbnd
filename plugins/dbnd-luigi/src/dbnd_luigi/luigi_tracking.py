import logging
import os

from typing import Type
from weakref import WeakValueDictionary

import luigi
import luigi.scheduler
import luigi.worker

from luigi.interface import _WorkerSchedulerFactory

import databand.parameters

from dbnd import Task, parameter
from dbnd._core.constants import RunState, TaskRunState
from dbnd._core.context.databand_context import DatabandContext
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled
from dbnd._core.run.databand_run import new_databand_run
from dbnd._core.task_build.task_definition import (
    _get_source_file,
    _get_task_module_source_code,
    _get_task_source_code,
)
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd._core.tracking.histograms import HistogramRequest
from targets import target


logger = logging.getLogger(__name__)


class _LuigiTask(Task):
    _luigi_task_completed = parameter(system=True, default=False)[bool]

    def _complete(self):
        return self._luigi_task_completed


def _luigi_task_cls_to_dbnd_task_cls(luigi_task_cls, luigi_task):
    # type: ( Type[luigi.Task], luigi.Task)-> Type[_LuigiTask]
    # create "root task" with default name as current process executable file name

    task_family = luigi_task_cls.get_task_family()
    attributes = {"_conf__task_family": task_family}

    _extract_parameters(attributes, luigi_task_cls)
    _extract_targets(attributes, luigi_task)

    logger.info("Creating new class %s", task_family)
    dbnd_task_cls = TaskMetaclass(str(task_family), (_LuigiTask,), attributes)

    try:
        dbnd_task_cls.task_definition.task_source_code = _get_task_source_code(
            luigi_task_cls
        )
        dbnd_task_cls.task_definition.task_module_code = _get_task_module_source_code(
            luigi_task_cls
        )
        dbnd_task_cls.task_definition.task_source_file = _get_source_file(
            luigi_task_cls
        )
    except Exception as ex:
        logger.info("Failed to find source code: %s", str(ex))

    return dbnd_task_cls


def _extract_parameters(attributes, luigi_task_cls):
    luigi_params = luigi_task_cls.get_params()
    for param_name, param_obj in luigi_params:
        attributes[param_name] = _convert_parameter_to_dbnd_parameter(param_obj)


def _extract_targets(attributes, luigi_task):
    def iterate_targets(target_or_targets, is_output, name=None):
        if isinstance(target_or_targets, dict):
            # Multiple targets, dict object
            for name, val in target_or_targets.items():
                iterate_targets(val, is_output, name)

        elif isinstance(target_or_targets, list):
            for t in target_or_targets:
                iterate_targets(t, is_output)
        else:
            # Single target, target object
            extract_target(target_or_targets, is_output, name)

    def extract_target(t, is_output, name):
        if not t:
            return
        dbnd_target_param = _luigi_target_to_dbnd_target_param(
            t, luigi_task.task_id, is_output=is_output
        )
        if not dbnd_target_param:
            return
        # TODO: Decide on parameter naming mechanism to prevent overwriting of two targets with same basename or name
        parameter_name = name or os.path.basename(t.path)
        counter = 0
        while parameter_name in attributes:
            if counter > 0:
                parameter_name = parameter_name[:-1] + str(counter)
            else:
                parameter_name = parameter_name + str(counter)
            counter += 1
        attributes[parameter_name] = dbnd_target_param

    iterate_targets(luigi_task.output(), is_output=True)
    iterate_targets(luigi_task.input(), is_output=False)


def _luigi_target_to_dbnd_target_param(luigi_target, task_id, is_output):
    from dbnd._core.task_ctrl.task_relations import traverse_and_set_target
    from targets.base_target import TargetSource
    from luigi.target import FileSystemTarget

    # TODO: Determine filesystem properly, currently relying on path
    if isinstance(luigi_target, FileSystemTarget):
        dbnd_target = target(luigi_target.path)
        fixed_target = traverse_and_set_target(
            dbnd_target, TargetSource(task_id=task_id)
        )
        target_param = (
            _get_dbnd_param_by_luigi_name("TargetParameter")
            .target_config(fixed_target.config)
            .default(fixed_target.path)()
        )
        if is_output:
            target_param = target_param.output()
        return target_param


def _get_dbnd_param_by_luigi_name(luigi_param_name):
    parameter_store = databand.parameters.__dict__
    return parameter_store.get(luigi_param_name, None)


def _convert_parameter_to_dbnd_parameter(luigi_param):
    matching_dbnd_parameter = _get_dbnd_param_by_luigi_name(
        luigi_param.__class__.__name__
    )
    if not matching_dbnd_parameter:
        logger.warning(
            "Could not convert luigi parameter {0} to dbnd parameter!".format(
                luigi_param.__class__.__name__
            )
        )
        return None

    matching_dbnd_parameter = matching_dbnd_parameter(
        default=luigi_param._default, description=luigi_param.description
    )  # Instantiate new object to prevent overwriting

    return matching_dbnd_parameter


def _luigi_task_to_dbnd_task(luigi_task):
    # type: ( luigi.Task)-> _LuigiTask
    dbnd_task_cls = _luigi_task_cls_to_dbnd_task_cls(luigi_task.__class__, luigi_task)

    dbnd_task_instance = dbnd_task_cls(
        task_name=luigi_task.get_task_family(), **luigi_task.param_kwargs
    )

    return dbnd_task_instance


class Singleton(type):
    _instances = WeakValueDictionary()

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            # This variable declaration is required to force a
            # strong reference on the instance.
            instance = super(Singleton, cls).__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

    @classmethod
    def clear(cls):
        for i in cls._instances:
            del i
        cls._instances = WeakValueDictionary()


def handle_postgres_histogram_logging(luigi_task):
    from dbnd_postgres.postgres_config import PostgresConfig

    conf = PostgresConfig()
    if not conf.auto_log_pg_histograms:
        return
    postgres_target = luigi_task.output()
    from dbnd._core.commands.metrics import log_pg_table

    log_pg_table(
        table_name=postgres_target.table,
        connection_string="postgres://{}:{}@{}:{}/{}".format(
            postgres_target.user,
            postgres_target.password,
            postgres_target.host,
            postgres_target.port,
            postgres_target.database,
        ),
        with_histograms=HistogramRequest.DEFAULT(),
    )


def should_log_pg_histogram(luigi_task):
    if not is_plugin_enabled("dbnd-postgres", module_import="dbnd_postgres"):
        return False
    try:
        from luigi.contrib.postgres import PostgresQuery
    except ImportError:
        return False
    return isinstance(luigi_task, PostgresQuery)


# TODO: Possibly fix metaclass syntax for python2
class LuigiRunManager(metaclass=Singleton):
    def __init__(self):
        self._context_managers = []

        # context exists
        self.databand_context = None

        # run exists
        self.databand_run = None

        self.events_active = False

        self._current_execution_context = None

        self.driver_task_run = None
        self.root_dbnd_task = None
        self._failed = False
        self.task_cache = {}

    def _initialize_dbnd_context(self):
        self.databand_context = DatabandContext(name="luigi")
        # Must enter context before creating dbnd task!
        dc = self._enter_cm(DatabandContext.context(_context=self.databand_context))

    def _enter_databand_run(self, scheduled_tasks=None):
        if not self.databand_context:
            self._initialize_dbnd_context()
            if scheduled_tasks and not self.root_dbnd_task:
                # This code is reached only when an orphaned task is executed
                self.root_dbnd_task = self.get_dbnd_task(
                    next(iter(scheduled_tasks.values()))
                )

        self.databand_run = self._enter_cm(
            new_databand_run(
                context=self.databand_context, task_or_task_name=self.root_dbnd_task
            )
        )
        # Start the tracking process
        self.databand_run._init_without_run()
        self.root_task_run = self.databand_run.root_task_run
        self.driver_task_run = self.databand_run.driver_task_run

        self._enter_cm(self.driver_task_run.runner.task_run_execution_context())
        self.driver_task_run.set_task_run_state(state=TaskRunState.RUNNING)

    def _build_dbnd_task(self, luigi_task):
        dbnd_task = _luigi_task_to_dbnd_task(luigi_task)
        self.task_cache[luigi_task.task_id] = (luigi_task, dbnd_task)
        return dbnd_task

    def _enter_cm(self, cm):
        val = cm.__enter__()
        self._context_managers.append(cm)
        return val

    def _close_all_context_managers(self):
        while self._context_managers:
            cm = self._context_managers.pop()
            cm.__exit__(None, None, None)

    def get_dbnd_task(self, luigi_task):
        if not self.databand_context:
            self._initialize_dbnd_context()

        if luigi_task.task_id in self.task_cache:
            dbnd_task = self.task_cache[luigi_task.task_id][1]
        else:
            dbnd_task = self._build_dbnd_task(luigi_task)
        return dbnd_task

    def _finish_run(self, task_run_state, error=None):
        self._failed = error is not None
        self.driver_task_run.set_task_run_state(state=task_run_state, error=error)
        self.databand_run.set_run_state(
            RunState.FAILED
            if task_run_state != TaskRunState.SUCCESS
            else RunState.SUCCESS
        )
        self._close_all_context_managers()

    def _init_databand_run(self, scheduled_tasks=None):
        if not self.databand_run:
            self._enter_databand_run(scheduled_tasks)

    def _is_luigi_task_discovered(self, luigi_task):
        return luigi_task.task_id in self.task_cache

    #################
    # LUIGI HANDLERS
    #################

    def on_run_start(self, luigi_task):
        # implicitly creates dbnd context if not exists
        dbnd_task = self.get_dbnd_task(luigi_task)
        if not self.root_dbnd_task:
            # If no root was defined until now, and a task is starting to run, it is the root task, and has no
            # dependencies (orphan)
            self.root_dbnd_task = dbnd_task

        self._current_execution_context = (
            dbnd_task.current_task_run.runner.task_run_execution_context()
        )
        self._current_execution_context.__enter__()

        dbnd_task.current_task_run.set_task_run_state(state=TaskRunState.RUNNING)

    def on_success(self, luigi_task):
        if should_log_pg_histogram(luigi_task):
            handle_postgres_histogram_logging(luigi_task)

        dbnd_task = self.get_dbnd_task(luigi_task)
        dbnd_task.current_task_run.set_task_run_state(state=TaskRunState.SUCCESS)
        if self._current_execution_context:
            self._current_execution_context.__exit__(None, None, None)

        if self.root_dbnd_task == dbnd_task and not self._failed:
            # No failure recorded, this is the last task in execution
            self._finish_run(TaskRunState.SUCCESS)

    def on_failure(self, luigi_task, exc):
        from dbnd._core.task_run.task_run_error import TaskRunError

        dbnd_task = self.get_dbnd_task(luigi_task)
        task_run_error = TaskRunError.build_from_ex(exc, dbnd_task.current_task_run)
        dbnd_task.current_task_run.set_task_run_state(
            state=TaskRunState.FAILED, error=task_run_error
        )

        if self._current_execution_context:
            self._current_execution_context.__exit__(None, None, None)

        if self.root_dbnd_task == dbnd_task:
            self._finish_run(TaskRunState.FAILED, error=task_run_error)

    def on_dependency_discovered(self, father_task, child_task):
        """
        Dependency will be run.
        This event is triggered before "START" is triggered
        """
        logger.info(
            "Dependency discovered: %s - > %s ", father_task.task_id, child_task.task_id
        )

        # TODO: How will databand know how many tasks there are before being run? Print graph?
        father_dbnd_task = self.get_dbnd_task(father_task)
        child_dbnd_task = self.get_dbnd_task(child_task)

        if not self.root_dbnd_task:
            self.root_dbnd_task = father_dbnd_task

        father_dbnd_task.set_upstream(child_dbnd_task)
        father_dbnd_task.task_meta.add_child(child_dbnd_task.task_id)

    def on_dependency_present(self, luigi_task):
        """
        Dependency is already found. This could either mean that dependency is complete, or was already found by Luigi
        """
        # If dependency was already discovered - do not mark luigi task as completed
        if not self._is_luigi_task_discovered(luigi_task):
            dbnd_task = self.get_dbnd_task(luigi_task)
            dbnd_task._luigi_task_completed = True

            if not self.root_dbnd_task:
                self.root_dbnd_task = dbnd_task

    def on_dependency_missing(self, luigi_task):
        logger.warning(
            "Encountered missing dependency for task {0}".format(luigi_task.task_id)
        )

    def on_progress(self):
        # TODO: Figure out if we can use _DbndWorker to send custom events for tracking
        """
        For user's use, is never called natively
        """
        pass


# Singleton
lrm = LuigiRunManager()


def _register_event(event, callback, target_cls=luigi.Task):
    target_cls._event_callbacks.setdefault(target_cls, {}).setdefault(event, set()).add(
        callback
    )


def _unregsiter_event(event, callback, target_cls=luigi.Task):
    target_cls._event_callbacks[target_cls][event].remove(callback)
    pass


def register_luigi_tracking():
    global lrm

    def _regsiter_luigi_tracking():
        _register_event(luigi.Event.START, lrm.on_run_start)
        _register_event(luigi.Event.SUCCESS, lrm.on_success)
        _register_event(luigi.Event.FAILURE, lrm.on_failure)
        _register_event(luigi.Event.DEPENDENCY_DISCOVERED, lrm.on_dependency_discovered)
        _register_event(luigi.Event.DEPENDENCY_MISSING, lrm.on_dependency_missing)
        _register_event(luigi.Event.DEPENDENCY_PRESENT, lrm.on_dependency_present)
        _register_event(luigi.Event.PROGRESS, lrm.on_progress)
        lrm.events_active = True

    def _unregsiter_luigi_tracking():
        _unregsiter_event(luigi.Event.START, lrm.on_run_start)
        _unregsiter_event(luigi.Event.SUCCESS, lrm.on_success)
        _unregsiter_event(luigi.Event.FAILURE, lrm.on_failure)
        _unregsiter_event(
            luigi.Event.DEPENDENCY_DISCOVERED, lrm.on_dependency_discovered
        )
        _unregsiter_event(luigi.Event.DEPENDENCY_MISSING, lrm.on_dependency_missing)
        _unregsiter_event(luigi.Event.DEPENDENCY_PRESENT, lrm.on_dependency_present)
        _unregsiter_event(luigi.Event.PROGRESS, lrm.on_progress)
        lrm.events_active = False

    if not lrm.events_active:
        # Fresh instance of run manager, no need to unlink event callbacks
        _regsiter_luigi_tracking()
    else:
        # 'Used' instance of run manager, need to unlink all event callbacks
        _unregsiter_luigi_tracking()
        Singleton.clear()
        lrm = LuigiRunManager()
        _regsiter_luigi_tracking()


# B.  Approach:  via overrides
# Challenges:
#   can's see the whole flow
# Good:
#    easy to integrate
class _DbndWorkerSchedulerFactory(_WorkerSchedulerFactory):
    def create_local_scheduler(self):
        return luigi.scheduler.Scheduler(
            prune_on_get_work=True, record_task_history=False
        )

    def create_worker(self, scheduler, worker_processes, assistant=False):
        return _DbndWorker(
            scheduler=scheduler, worker_processes=worker_processes, assistant=assistant
        )


class _DbndScheduler(luigi.scheduler.Scheduler):
    pass


class _DbndWorker(luigi.worker.Worker):
    def run(self):
        # Passing currently scheduled tasks just in case we are running an orphaned task and need a root task template
        lrm._init_databand_run(self._scheduled_tasks)
        """
        _init_databand_run is called right before worker.run executes.
        Worker.run() when working in multiprocess calls `fork()`.
        Our best method of supplying the databand run object to all subsequent subprocesses is initializing it before
        the worker processes split.
        """
        return super(_DbndWorker, self).run()


def _set_luigi_kwargs(kwargs):
    kwargs.setdefault("worker_scheduler_factory", _DbndWorkerSchedulerFactory())
    kwargs.setdefault("detailed_summary", True)
    kwargs.setdefault("local_scheduler", True)
    return kwargs


def dbnd_luigi_track():
    register_luigi_tracking()


def dbnd_luigi_run(**kwargs):
    register_luigi_tracking()

    kwargs = _set_luigi_kwargs(kwargs)
    run_result = luigi.run(**kwargs)
    return run_result


def dbnd_luigi_build(tasks, **kwargs):
    dbnd_luigi_track()
    kwargs = _set_luigi_kwargs(kwargs)
    run_result = luigi.build(tasks=tasks, **kwargs)
    return run_result
