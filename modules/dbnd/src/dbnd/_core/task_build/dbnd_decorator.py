import logging
import typing

import six

from dbnd._core.configuration.environ_config import is_databand_enabled
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task.decorated_callable_task import (
    DecoratedPipelineTask,
    DecoratedPythonTask,
)
from dbnd._core.task_build.task_decorator import (
    TaskDecorator,
    _UserClassWithTaskDecoratorMetaclass,
    build_dbnd_decorated_func,
)
from dbnd._core.task_build.task_registry import get_task_registry
from dbnd._core.tracking.managers.callable_tracking import _do_nothing_decorator


if typing.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)
_default_output = parameter.output.pickle[object]


def task(*args, **kwargs):
    kwargs.setdefault("_task_type", DecoratedPythonTask)
    kwargs.setdefault("_task_default_result", _default_output)
    return build_task_decorator(*args, **kwargs)


def pipeline(*args, **kwargs):
    kwargs.setdefault("_task_type", DecoratedPipelineTask)
    kwargs.setdefault("_task_default_result", parameter.output)
    return build_task_decorator(*args, **kwargs)


band = pipeline
data_source_pipeline = pipeline


def build_task_decorator(*decorator_args, **decorator_kwargs):
    # this code creates a new decorator that can be applied on any User Code

    if not is_databand_enabled():
        # simple `@task` decorator, no options were (probably) given.
        if len(decorator_args) == 1 and callable(decorator_args[0]):
            return decorator_args[0]
        return _do_nothing_decorator

    def class_or_func_decorator(class_or_func):
        # this code will run during compile time, when we apply dbnd decorator (for example: @task)
        task_decorator = TaskDecorator(class_or_func, decorator_kwargs=decorator_kwargs)
        tp = task_decorator.task_passport

        # we need to manually register the task here, since in regular flow
        # this happens in TaskMetaclass, but it's not invoked here due to lazy
        # evaluation task_cls
        r = get_task_registry()
        r.register_task_cls_factory(
            task_cls_factory=task_decorator.get_task_cls,
            full_task_family=tp.full_task_family,
            task_family=tp.task_family,
        )
        if task_decorator.is_class:
            # we will change metaclass for UserClass so we will process all UserClass calls
            #
            # @task
            # class UserClass():
            #     pass
            # so the the moment user call UserClass(), -> _DecoratedUserClassMeta.__call__ will be called
            dbnd_decorated_class = six.add_metaclass(
                _UserClassWithTaskDecoratorMetaclass
            )(class_or_func)
            dbnd_decorated_class.task_decorator = task_decorator
            task_decorator.class_or_func = dbnd_decorated_class
            return dbnd_decorated_class
        else:
            # @task
            # def user_func():
            #     pass
            # we will return our wrapper, that will be called during a runtime,
            # when user calls his own code.
            return build_dbnd_decorated_func(task_decorator)

    # simple `@task` decorator in opposite to @task(...), no options were (probably) given.
    if len(decorator_args) == 1 and callable(decorator_args[0]):
        return class_or_func_decorator(decorator_args[0])

    return class_or_func_decorator
