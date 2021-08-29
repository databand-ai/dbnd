import functools
import inspect
import logging
import typing

from typing import Any, Optional, Type

from dbnd._core.configuration.environ_config import get_dbnd_project_config
from dbnd._core.current import current_task_run, get_databand_run, try_get_current_task
from dbnd._core.errors import show_exc_info
from dbnd._core.errors.errors_utils import user_side_code
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.plugin.dbnd_airflow_operator_plugin import (
    build_task_at_airflow_dag_context,
    is_in_airflow_dag_build_context,
)
from dbnd._core.task.decorated_callable_task import _DecoratedCallableTask
from dbnd._core.task_build.task_context import TaskContextPhase, current_phase
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd._core.task_build.task_passport import TaskPassport
from dbnd._core.tracking.managers.callable_tracking import CallableTrackingManager
from dbnd._core.utils.basics.nothing import NOTHING
from dbnd._core.utils.callable_spec import (
    CallableSpec,
    args_to_kwargs,
    build_callable_spec,
)
from dbnd._core.utils.lazy_property_proxy import CallableLazyObjectProxy
from targets.inline_target import InlineTarget


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd import Task
    from dbnd._core.task_run.task_run import TaskRun


class TaskDecorator(object):
    """
    This object represent the state and logic of decorated callable (user class or user function)
    All "expensive" objects are lazy: task_cls, callable_spec, callable_tracking_manager

    all "user-side" calls are routed into self.handle_callable_call() that will decide should we
    1. call user code directly (dbnd is disabled)
    2. call and track user callable call (tracking is enabled)
    3. create a Task that represents user code ( orchestration mode at @pipeline.band
    4. create a Task and run it (orchestration mode at @task.run)
    """

    def __init__(self, class_or_func, decorator_kwargs):
        # known parameters for @task
        self.class_or_func = class_or_func
        self.original_class_or_func = class_or_func

        self.task_type = decorator_kwargs.pop(
            "_task_type"
        )  # type: Type[_DecoratedCallableTask]
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
        self._task_cls = None  # type: Optional[Type[Task]]
        self._callable_spec = None  # type: Optional[CallableSpec]
        self._callable_tracking_manager = (
            None
        )  # type: Optional[CallableTrackingManager]

    def get_callable_spec(self):
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
        if not self._callable_tracking_manager:
            self._callable_tracking_manager = CallableTrackingManager(
                task_decorator=self
            )
        return self._callable_tracking_manager.tracking_context(
            call_args=call_args, call_kwargs=call_kwargs
        )

    def handle_callable_call(self, *call_args, **call_kwargs):
        dbnd_project_config = get_dbnd_project_config()
        if dbnd_project_config.disabled:
            return self.class_or_func(*call_args, **call_kwargs)

        # we are at tracking mode
        if dbnd_project_config.is_tracking_mode():
            with self.tracking_context(call_args, call_kwargs) as track_result_callback:
                fp_result = self.class_or_func(*call_args, **call_kwargs)
                return track_result_callback(fp_result)

        #### DBND ORCHESTRATION MODE
        #
        #     -= Use "Step into My Code"" to get back from dbnd code! =-
        #
        # decorated object call/creation  ( my_func(), MyDecoratedTask()
        # we are at orchestration mode

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
        # current is not None, and we are not in tracking/airflow/luigi
        # this is DBND Orchestration mode
        # we can be in the context of task.run() or in task.band()
        # called from user code using user_decorated_func()  or UserDecoratedTask()

        if self.is_class:
            call_kwargs.pop("__call_original_cls", False)

        # we should not get here from _TaskFromTaskDecorator.invoke()
        # at that function we should call user code directly
        phase = current_phase()
        if phase is TaskContextPhase.BUILD:
            # we are in the @pipeline.band() context, we are building execution plan
            t = task_cls(*call_args, **call_kwargs)

            # we are in the band, and if user_code() is called we want to remove redundant
            # `user_code().result` usage
            if t.task_definition.single_result_output:
                return t.result

            # we have multiple outputs (more than one "output" parameter)
            # just return task object, user will use it as `user_code().output_1`
            return t
        elif phase is TaskContextPhase.RUN:
            # we are "running" inside some other task execution (orchestration!)
            #  (inside user_defined_function() or UserDefinedTask.run()

            # if possible we will run it as "orchestration" task
            # with parameters parsing
            if (
                current.settings.run.task_run_at_execution_time_enabled
                and current.task_supports_dynamic_tasks
            ):
                return self._run_task_from_another_task_execution(
                    parent_task=current, call_args=call_args, call_kwargs=call_kwargs,
                )
            # we can not call it in "dbnd" way, fallback to normal call
            if self.is_class:
                call_kwargs["__call_original_cls"] = False
            return self.class_or_func(*call_args, **call_kwargs)
        else:
            raise Exception()

    def _run_task_from_another_task_execution(
        self, parent_task, call_args, call_kwargs
    ):
        # type: (TaskDecorator, Task, *Any, **Any) -> TaskRun
        # task is running from another task
        task_cls = self.get_task_cls()
        from dbnd import pipeline, PipelineTask
        from dbnd._core.task_build.dbnd_decorator import _default_output

        dbnd_run = get_databand_run()

        # orig_call_args, orig_call_kwargs = call_args, call_kwargs
        call_args, call_kwargs = args_to_kwargs(
            self.get_callable_spec().args, call_args, call_kwargs
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
            "task_in_memory_outputs",
            parent_task.settings.run.task_run_at_execution_time_in_memory_outputs,
        )

        if issubclass(task_cls, PipelineTask):
            # if it's pipeline - create new databand run
            # create override _task_default_result to be object instead of target
            task_cls = pipeline(
                self.class_or_func, _task_default_result=_default_output
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
                task_run = dbnd_run.run_executor.run_task_at_execution_time(
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

    @property
    def func(self):
        return self.class_or_func

    @property
    def callable(self):
        return self.class_or_func


class _UserClassWithTaskDecoratorMetaclass(type):
    """
    Used by decorated user classes only,
    1. we change metaclass of original class to go through __call__ on object call
    2. object still behaves as original object (until __call_ is called)
    3. we intercept the call and may call original object, or create dbnd task class
    (at @pipeline or inside @task.run function)

    in order to prevent recursion ( from DecoratedCallableTask.invoke for example)
    we use `__call_original_cls` kwarg. if present we would call an original code

    this code should be serializable with pickle!

    @task
    class UserClass():
        pass
    """

    __is_dbnd_task__ = True

    task_decorator = None  # type: TaskDecorator

    def __call__(cls, *args, **kwargs):
        """
        wrap user class ,so on user_class() we run _item_call first and if required we return task object inplace
        """
        if kwargs.pop("__call_original_cls", False):
            return super(_UserClassWithTaskDecoratorMetaclass, cls).__call__(
                *args, **kwargs
            )

        # prevent recursion call. next time we call cls() we will go into original ctor()
        kwargs["__call_original_cls"] = True
        return cls.task_decorator.handle_callable_call(*args, **kwargs)

    # exposing dbnd logic
    # so OriginalUserClass.task can be used
    # this list should be alligned with attributes at build_dbnd_decorated_func

    @property
    def task_cls(self):
        return self.task_decorator.get_task_cls()

    @property
    def t(self):
        return self.task_cls

    @property
    def task(self):
        return self.task_cls

    @property
    def func(self):
        return self.task_decorator.callable

    @property
    def callable(self):
        return self.task_decorator.callable

    def dbnd_run(self, *args, **kwargs):
        return self.task_decorator.dbnd_run(*args, **kwargs)


def build_dbnd_decorated_func(task_decorator):
    def dbnd_decorated_func(*args, **kwargs):
        """
        DBND Wrapper of User Function
        Redirect call to dbnd logic that might track/orchestrate user code
        """
        return task_decorator.handle_callable_call(*args, **kwargs)

    # new wrapper should should look like original function and be serializable
    # class decorator will not work, because of pickle errors
    functools.update_wrapper(dbnd_decorated_func, task_decorator.original_class_or_func)

    # this list should be alligned with attributes at _UserClassWithTaskDecoratorMetaclass

    # we don't want to create .task_cls object immediately(that is used for orchestration only)
    # however, we can't just return task_decorator, as it's class, and it can not be serialized
    # with pickle, as user func != task_decorator class
    # we wrap "all known" heavy properties with CallableLazyObjectProxy

    lazy_task_cls_property = CallableLazyObjectProxy(task_decorator.get_task_cls)
    # create lazy properties t, task and task_cls
    dbnd_decorated_func.task = lazy_task_cls_property
    dbnd_decorated_func.t = lazy_task_cls_property
    dbnd_decorated_func.task_cls = lazy_task_cls_property

    dbnd_decorated_func.func = task_decorator.callable  # backward compatiblity
    dbnd_decorated_func.callable = task_decorator.callable

    dbnd_decorated_func.dbnd_run = task_decorator.dbnd_run
    dbnd_decorated_func.task_decorator = task_decorator
    dbnd_decorated_func.__is_dbnd_task__ = True
    return dbnd_decorated_func
