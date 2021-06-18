import functools
import inspect
import logging
import typing

from typing import Any

from dbnd._core.configuration.environ_config import get_dbnd_project_config
from dbnd._core.current import current_task_run, get_databand_run, try_get_current_task
from dbnd._core.decorator.callable_spec import args_to_kwargs, build_callable_spec
from dbnd._core.errors import show_exc_info
from dbnd._core.errors.errors_utils import user_side_code
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.plugin.dbnd_airflow_operator_plugin import (
    build_task_at_airflow_dag_context,
    is_in_airflow_dag_build_context,
)
from dbnd._core.task_build.task_context import TaskContextPhase, current_phase
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd._core.task_build.task_passport import TaskPassport
from dbnd._core.tracking.managers.callable_tracking import CallableTrackingManager
from dbnd._core.utils.basics.nothing import NOTHING
from targets.inline_target import InlineTarget


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd import Task
    from dbnd._core.task_run.task_run import TaskRun


class TaskDecorator(object):
    __is_dbnd_task__ = True

    def __init__(self, class_or_func, decorator_kwargs):
        # known parameters for @task
        self.class_or_func = class_or_func
        self.original_class_or_func = class_or_func

        self.task_type = decorator_kwargs.pop(
            "_task_type"
        )  # type: Type[_TaskFromTaskDecorator]
        self.task_default_result = decorator_kwargs.pop(
            "_task_default_result"
        )  # ParameterFactory
        self.task_defaults = decorator_kwargs.pop("defaults", None)

        self.task_namespace = decorator_kwargs.get("task_namespace", NOTHING)
        self.task_family = decorator_kwargs.get("_conf__task_family")

        # rest of kwargs are "user params"
        self.decorator_kwargs = decorator_kwargs

        self.task_passport = TaskPassport.build_task_passport(
            cls_name=self.original_class_or_func.__name__,
            module_name=self.original_class_or_func.__module__,
            task_namespace=self.task_namespace,
            task_family=self.task_family,
        )

        self.is_class = inspect.isclass(class_or_func)

        # used by decorated UserClass only, stores "wrapped" user class

        # lazy task class definition for orchestration case
        self._task_cls = None
        self._callable_spec = None

        functools.update_wrapper(self, class_or_func)

    def get_func_spec(self):
        if not self._callable_spec:
            try:
                self._callable_spec = build_callable_spec(
                    class_or_func=self.original_class_or_func
                )
            except Exception as ex:
                logger.error(
                    "Failed to create task %s: %s\n%s\n",
                    self.original_class_or_func.__name__,
                    str(ex),
                    user_side_code(context=5),
                    exc_info=show_exc_info(ex),
                )
                raise
        return self._callable_spec

    def get_task_cls(self):
        """
        Returns Runnable Task for Orchestration
        """
        if self._task_cls is None:
            # Use the task_type we got from the decorator. check @task/@pipeline/@spark_task
            bases = (self.task_type,)

            self._task_cls = TaskMetaclass(
                str(self.original_class_or_func.__name__),
                bases,
                dict(
                    __doc__=self.original_class_or_func.__doc__,
                    __module__=self.original_class_or_func.__module__,
                    defaults=self.task_defaults,
                    task_decorator=self,
                ),
            )
        return self._task_cls

    @property
    def func(self):
        return self.class_or_func

    def get_task_definition(self):
        return self.get_task_cls().task_definition

    @dbnd_handle_errors(exit_on_error=False)
    def _build_task(self, *args, **kwargs):
        task_cls = self.get_task_cls()
        return task_cls(*args, **kwargs)

    @dbnd_handle_errors(exit_on_error=False)
    def dbnd_run(self, *args, **kwargs):
        # type: (...)-> DatabandRun
        """
        Run task via Databand execution system
        """
        t = self._build_task(*args, **kwargs)
        return t.dbnd_run()

    def tracking_context(self, call_args, call_kwargs):
        mngr = CallableTrackingManager(task_decorator=self)
        return mngr.tracking_context(call_args=call_args, call_kwargs=call_kwargs)

    def __call__(self, *args, **kwargs):
        dbnd_project_config = get_dbnd_project_config()
        if dbnd_project_config.disabled:
            return self.class_or_func(*args, **kwargs)

        # we are at tracking mode
        if dbnd_project_config.is_tracking_mode():
            with self.tracking_context(args, kwargs) as track_result_callback:
                fp_result = self.class_or_func(*args, **kwargs)
                return track_result_callback(fp_result)

        #### DBND ORCHESTRATION MODE
        #
        #     -= Use "Step into My Code"" to get back from dbnd code! =-
        #
        # decorated object call/creation  ( my_func(), MyDecoratedTask()
        # we are at orchestration mode

        call_args = args
        call_kwargs = kwargs
        task_cls = self.get_task_cls()

        if is_in_airflow_dag_build_context():
            # we are in Airflow DAG building mode - AIP-31
            return build_task_at_airflow_dag_context(
                task_cls=task_cls, call_args=call_args, call_kwargs=call_kwargs
            )

        current = try_get_current_task()
        if not current:
            # no tracking/no orchestration,
            # falling back to "natural call" of the class_or_func
            return self.class_or_func(*call_args, **call_kwargs)

        ######
        # current is not None, and we are not in trackign/airflow/luigi
        # DBND Orchestration mode
        # we can be in the context of .run() or in .band()
        # called from  user code using user_decorated_func()  or UserDecoratedTask()

        # we should not get here from _TaskFromTaskDecorator.invoke()
        # at that function we should call user code directly
        phase = current_phase()
        if phase is TaskContextPhase.BUILD:
            # we are in the @pipeline.band() context, we are building execution plan
            t = task_cls(*call_args, **call_kwargs)

            # we are in the band
            if t.task_definition.single_result_output:
                return t.result

            # we have multiple outputs ( result, another output.. )
            # -> just return task object
            return t
        elif phase is TaskContextPhase.RUN:
            # we are "running" inside some other task execution (orchestration!)
            #  (inside user_defined_function() or UserDefinedTask.run()

            # if possible we will run it as "orchestration" task
            # with parameters parsing
            if (
                current.settings.dynamic_task.enabled
                and current.task_supports_dynamic_tasks
            ):
                return _task_cls__call__in_scope_of_orchestration_run(
                    task_decorator=self,
                    parent_task=current,
                    call_args=call_args,
                    call_kwargs=call_kwargs,
                )
            # we can not call it in "dbnd" way, fallback to normal call
            return self.class_or_func(*call_args, **call_kwargs)
        else:
            raise Exception()

    # compatibility support
    @property
    def task_cls(self):
        return self.get_task_cls()

    @property
    def t(self):
        return self.get_task_cls()

    @property
    def task(self):
        return self.get_task_cls()

    @property
    def task_definition(self):
        return self.get_task_definition()


class _DecoratedUserClassMeta(type):
    """
    Used by decorated user classes only,
    we change metaclass of original class to go through dbnd as a proxy
    @task
    class UserClass():
        pass
    """

    def __call__(cls, *args, **kwargs):
        """
        wrap user class ,so on user_class() we run _item_call first and if required we return task object inplace
        """
        if kwargs.pop("__call_original_cls", False):
            return super(_DecoratedUserClassMeta, cls).__call__(*args, **kwargs)
        kwargs["__call_original_cls"] = True
        return cls.task_decorator(*args, **kwargs)

    # compatibility support

    @property
    def task_cls(self):
        return self.task_decorator.get_task_cls()

    @property
    def t(self):
        return self.task_cls

    @property
    def task(self):
        return self.task_cls


def _task_cls__call__in_scope_of_orchestration_run(
    task_decorator, parent_task, call_args, call_kwargs
):
    # type: (TaskDecorator, Task, *Any, **Any) -> TaskRun
    # task is running from another task
    task_cls = task_decorator.get_task_cls()
    if task_decorator.is_class:
        call_kwargs.pop("__call_original_cls", False)
    from dbnd import pipeline, PipelineTask
    from dbnd._core.decorator.dbnd_decorator import _default_output

    dbnd_run = get_databand_run()

    # orig_call_args, orig_call_kwargs = call_args, call_kwargs
    call_args, call_kwargs = args_to_kwargs(
        task_decorator.get_func_spec().args, call_args, call_kwargs
    )

    # Map all kwargs to the "original" target of that objects
    # for example: for DataFrame we'll try to find a relevant target that were used to read it
    # get all possible value's targets
    call_kwargs_as_targets = dbnd_run.target_origin.get_for_map(call_kwargs)
    for p_name, value_origin in call_kwargs_as_targets.items():
        root_target = value_origin.origin_target
        path = root_target.path if hasattr(root_target, "path") else None
        original_object = call_kwargs[p_name]
        call_kwargs[p_name] = InlineTarget(
            root_target=root_target,
            obj=original_object,
            value_type=value_origin.value_type,
            source=value_origin.origin_target.source,
            path=path,
        )

    call_kwargs.setdefault("task_is_dynamic", True)
    call_kwargs.setdefault(
        "task_in_memory_outputs", parent_task.settings.dynamic_task.in_memory_outputs
    )

    if issubclass(task_cls, PipelineTask):
        # if it's pipeline - create new databand run
        # create override _task_default_result to be object instead of target
        task_cls = pipeline(
            task_decorator.class_or_func, _task_default_result=_default_output
        ).task_cls

        # instantiate inline pipeline
        task = task_cls(*call_args, **call_kwargs)
        # if it's pipeline - create new databand run
        run = dbnd_run.context.dbnd_run_task(task)
        task_run = run.get_task_run(task.task_id)
    else:
        # instantiate inline task (dbnd object)
        task = task_cls(*call_args, **call_kwargs)

        # update upstream/downstream relations - needed for correct tracking
        # we can have the task as upstream , as it was executed already
        if not parent_task.task_dag.has_upstream(task):
            parent_task.set_upstream(task)

        from dbnd._core.task_build.task_cls__call_state import TaskCallState

        task._dbnd_call_state = TaskCallState(should_store_result=True)
        try:
            task_run = dbnd_run.run_executor.run_dynamic_task(
                task, task_engine=current_task_run().task_engine
            )

            # this will work only for _DecoratedTask
            if task._dbnd_call_state.result_saved:
                return task._dbnd_call_state.result

        finally:
            # we'd better clean _invoke_result to avoid memory leaks
            task._dbnd_call_state = None

    # if we are inside run, we want to have real values, not deferred!
    if task.task_definition.single_result_output:
        return task.__class__.result.load_from_target(task.result)
        # we have func without result, just fallback to None
    return task
