import functools
import logging
import typing

from typing import Any, Type

import six

from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.configuration.environ_config import (
    in_tracking_mode,
    is_databand_enabled,
)
from dbnd._core.current import try_get_current_task
from dbnd._core.decorator.decorated_task import _DecoratedTask
from dbnd._core.decorator.dynamic_tasks import create_and_run_dynamic_task_safe
from dbnd._core.decorator.func_task_call import FuncCall
from dbnd._core.decorator.lazy_object_proxy import CallableLazyObjectProxy
from dbnd._core.decorator.task_decorator_spec import (
    _TaskDecoratorSpec,
    build_task_decorator_spec,
)
from dbnd._core.errors import show_exc_info
from dbnd._core.errors.errors_utils import user_side_code
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.plugin.dbnd_airflow_operator_plugin import (
    build_task_at_airflow_dag_context,
    is_in_airflow_dag_build_context,
)
from dbnd._core.task_build.task_context import (
    TaskContextPhase,
    current_phase,
    try_get_current_task,
)
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd._core.task_build.task_passport import TaskPassport
from dbnd._core.task_build.task_registry import get_task_registry
from dbnd._core.tracking.managers.callable_tracking import (
    CallableTrackingManager,
    _passthrough_decorator,
)
from dbnd._core.utils.basics.nothing import NOTHING


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun

logger = logging.getLogger(__name__)


class TaskClsBuilder(object):
    def __init__(self, func_spec, task_type, task_defaults, task_passport):
        # type: (TaskClsBuilder, _TaskDecoratorSpec, Type[_DecoratedTask], Any, TaskPassport) -> None
        self.func_spec = func_spec
        self.task_type = task_type
        self.task_passport = task_passport
        self.task_defaults = task_defaults

        self._normal_task_cls = None
        # self.task_cls = task_cls  # type: Type[Task]
        # this will make class look like a origin function
        functools.update_wrapper(self, self.func)

        self._callable_tracking_manager = CallableTrackingManager(
            func_spec=func_spec, task_defaults=task_defaults,
        )
        self._callable_item = None

    @property
    def func(self):
        return self.func_spec.item

    def get_task_cls(self):
        if self._normal_task_cls is None:
            self._normal_task_cls = self._build_task_cls()
        return self._normal_task_cls

    def get_task_definition(self):
        return self.get_task_cls().task_definition

    def _build_task_cls(self):
        """
        Returns Runnable Task
        """
        class_or_func = self.func

        # Use the task_type we got from the decorator. check @task/@pipeline/@spark_task
        bases = (self.task_type,)

        task_cls = TaskMetaclass(
            str(class_or_func.__name__),
            bases,
            dict(
                _conf__decorator_spec=self.func_spec,
                _callable_item=self._callable_item or class_or_func,
                __doc__=class_or_func.__doc__,
                __module__=class_or_func.__module__,
                defaults=self.task_defaults,
            ),
        )
        return task_cls

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


class _DecoratedUserClassMeta(type):
    def __call__(cls, *args, **kwargs):
        """
        wrap user class ,so on user_class() we run _item_call first and if required we return task object inplace
        """
        return _call_handler(
            cls.task_cls,
            call_user_code=super(_DecoratedUserClassMeta, cls).__call__,
            call_args=args,
            call_kwargs=kwargs,
        )


def _call_handler(task_cls, call_user_code, call_args, call_kwargs):
    """
    -= Use "Step into My Code"" to get back from Databand code! =-

    decorated object call/creation  ( my_func(), MyDecoratedTask()
    """
    force_invoke = call_kwargs.pop("__force_invoke", False)
    dbnd_project_config = get_dbnd_project_config()

    if force_invoke or dbnd_project_config.disabled:
        # 1. Databand is not enabled
        # 2. we have this call coming from Task.run / Task.band direct invocation
        return call_user_code(*call_args, **call_kwargs)
    func_call = FuncCall(
        task_cls=task_cls,
        call_args=call_args,
        call_kwargs=call_kwargs,
        call_user_code=call_user_code,
    )

    if is_in_airflow_dag_build_context():  # we are in Airflow DAG building mode
        return build_task_at_airflow_dag_context(
            task_cls=task_cls, call_args=call_args, call_kwargs=call_kwargs
        )

    current = try_get_current_task()
    if not current:
        from dbnd._core.tracking.script_tracking_manager import (
            try_get_inplace_tracking_task_run,
        )

        task_run = try_get_inplace_tracking_task_run()
        if task_run:
            current = task_run.task

    if not current:  # direct call to the function
        return func_call.invoke()

    ######
    # current is not None, and we are not in trackign/airflow/luigi
    # DBND Orchestration mode
    # we can be in the context of .run() or in .band()
    # called from  user code using some_func()  or SomeTask()
    # this call path is not coming from it's not coming from _invoke_func
    phase = current_phase()
    if phase is TaskContextPhase.BUILD:
        # we are in the @pipeline context, we are building execution plan
        t = task_cls(*call_args, **call_kwargs)

        # we are in inline debug mode -> we are going to execute the task
        # we are in the band
        # and want to return result of the object
        if t.task_definition.single_result_output:
            return t.result

        # we have multiple outputs ( result, another output.. )
        # -> just return task object
        return t

    if phase is TaskContextPhase.RUN:
        # we are in the run function!
        if (
            current.settings.dynamic_task.enabled
            and current.task_supports_dynamic_tasks
        ):
            # isinstance() check required to prevent infinite recursion when @task is on
            # class and not on func (example: see test_task_decorated_class.py)
            # and the current task supports inline calls
            # that's extra mechanism in addition to __force_invoke
            # on pickle/unpickle isinstance fails to run.
            return create_and_run_dynamic_task_safe(
                func_call=func_call, parent_task_run=current
            )

    # we can not call it in"databand" way, fallback to normal execution
    return func_call.invoke()


def _task_decorator(*decorator_args, **decorator_kwargs):
    if not is_databand_enabled():
        # simple `@task` decorator, no options were (probably) given.
        if len(decorator_args) == 1 and callable(decorator_args[0]):
            return decorator_args[0]
        return _passthrough_decorator

    task_type = decorator_kwargs.pop("_task_type")  # type: Type[_DecoratedTask]
    task_default_result = decorator_kwargs.pop(
        "_task_default_result"
    )  # ParameterFactory
    task_defaults = decorator_kwargs.pop("defaults", None)

    def decorated(class_or_func):
        try:
            func_spec = build_task_decorator_spec(
                class_or_func=class_or_func,
                decorator_kwargs=decorator_kwargs,
                default_result=task_default_result,
            )
        except Exception as ex:
            logger.error(
                "Failed to create task %s: %s\n%s\n",
                class_or_func.__name__,
                str(ex),
                user_side_code(context=5),
                exc_info=show_exc_info(ex),
            )
            raise

        task_namespace = decorator_kwargs.get("task_namespace", NOTHING)
        task_family = decorator_kwargs.get("_conf__task_family")

        tp = TaskPassport.from_func_spec(
            func_spec, task_namespace=task_namespace, task_family=task_family
        )
        fp = TaskClsBuilder(func_spec, task_type, task_defaults, task_passport=tp)

        if func_spec.is_class:
            wrapper = six.add_metaclass(_DecoratedUserClassMeta)(class_or_func)
            fp._callable_item = wrapper

        else:

            @functools.wraps(class_or_func)
            def wrapper(*args, **kwargs):
                if in_tracking_mode():
                    with fp._callable_tracking_manager.tracking_context(
                        args, kwargs
                    ) as track_result_callback:
                        fp_result = fp.func(*args, **kwargs)
                        return track_result_callback(fp_result)

                return _call_handler(
                    fp.get_task_cls(),
                    call_user_code=fp.func,
                    call_args=args,
                    call_kwargs=kwargs,
                )

            wrapper.dbnd_run = fp.dbnd_run

        wrapper.__is_dbnd_task__ = True
        wrapper.func = class_or_func

        # we're using CallableLazyObjectProxy to have lazy evaluation for creating task_cls
        # this is only orchestration scenarios
        task_cls = CallableLazyObjectProxy(fp.get_task_cls)
        wrapper.task_cls = task_cls
        wrapper.task = task_cls
        wrapper.t = task_cls

        # we need lazy task_definition here, for example for dbnd_task_as_bash_operator
        wrapper.task_definition = CallableLazyObjectProxy(fp.get_task_definition)

        # we need to manually register the task here, since in regular flow
        # this happens in TaskMetaclass, but it's not invoked here due to lazy
        # evaluation using CallableLazyObjectProxy
        # TODO: we can use CallableLazyObjectProxy object (task_cls) instead of task_cls_factory
        r = get_task_registry()
        r.register_task_cls_factory(
            task_cls_factory=fp.get_task_cls,
            full_task_family=tp.full_task_family,
            task_family=tp.task_family,
        )

        return wrapper

    # simple `@task` decorator, no options were (probably) given.
    if len(decorator_args) == 1 and callable(decorator_args[0]):
        return decorated(decorator_args[0])

    return decorated
