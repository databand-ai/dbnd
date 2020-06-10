import functools
import logging
import typing

from typing import Type

import six

from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.configuration.environ_config import is_databand_enabled
from dbnd._core.decorator.decorated_task import (
    DecoratedPipelineTask,
    DecoratedPythonTask,
    _DecoratedTask,
)
from dbnd._core.decorator.task_decorator_spec import build_task_decorator_spec
from dbnd._core.errors import show_exc_info
from dbnd._core.errors.errors_utils import user_side_code
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task.task import Task


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun

logger = logging.getLogger(__name__)
T = typing.TypeVar("T")

_default_output = parameter.output.pickle[object]


def task(*args, **kwargs):
    kwargs.setdefault("_task_type", DecoratedPythonTask)
    kwargs.setdefault("_task_default_result", _default_output)
    return _task_decorator(*args, **kwargs)


def pipeline(*args, **kwargs):
    kwargs.setdefault("_task_type", DecoratedPipelineTask)
    kwargs.setdefault("_task_default_result", parameter.output)
    return _task_decorator(*args, **kwargs)


band = pipeline


class _decorated_user_func(object):
    def __init__(self, task_cls):
        self.task_cls = task_cls  # type: Type[Task]
        # this will make class look like a origin function
        self.task_func = self.task_cls._conf__decorator_spec.item
        functools.update_wrapper(self, self.task_func)
        self._call_count = 0
        self._call_as_func = False
        self._max_call_count = get_dbnd_project_config().max_calls_per_run

    def __call__(self, *args, **kwargs):
        if not self._call_as_func:
            self._call_count += 1
            if self._call_count > self._max_call_count:
                logger.info(
                    "Reached maximum tracking limit of {} tasks. Running function regularly.".format(
                        self._max_call_count
                    )
                )
                self._call_as_func = True

        if self._call_as_func:
            return self.task_func(*args, **kwargs)

        return self.task_cls._call_handler(
            call_user_code=self.task_func, call_args=args, call_kwargs=kwargs
        )

    @dbnd_handle_errors(exit_on_error=False)
    def dbnd_run(self, *args, **kwargs):
        # type: (...)-> DatabandRun
        """
        Run task via Databand execution system
        """
        t = self._build_task(*args, **kwargs)
        return t.dbnd_run()

    @dbnd_handle_errors(exit_on_error=False)
    def _build_task(self, *args, **kwargs):
        return self.task_cls(*args, **kwargs)


class _DecoratedUserClassMeta(type):
    def __call__(cls, *args, **kwargs):
        """
        wrap user class ,so on user_class() we run _item_call first and if required we return task object inplace
        """
        return cls.task_cls._call_handler(
            call_user_code=super(_DecoratedUserClassMeta, cls).__call__,
            call_args=args,
            call_kwargs=kwargs,
        )


def __passthrough_decorator(f):
    return f


def _task_decorator(*decorator_args, **decorator_kwargs):
    task_type = decorator_kwargs.pop("_task_type")  # type: Type[_DecoratedTask]
    task_default_result = decorator_kwargs.pop(
        "_task_default_result"
    )  # ParameterFactory
    task_defaults = decorator_kwargs.pop("defaults", None)
    if not is_databand_enabled():
        # simple `@task` decorator, no options were (probably) given.
        if len(decorator_args) == 1 and callable(decorator_args[0]):
            return decorator_args[0]
        return __passthrough_decorator

    def decorated(item):
        try:

            func_spec = build_task_decorator_spec(
                item=item,
                decorator_kwargs=decorator_kwargs,
                default_result=task_default_result,
            )

            class DynamicFuncAsTask(task_type):
                _conf__decorator_spec = func_spec
                _callable_item = None
                __doc__ = item.__doc__
                __module__ = item.__module__

                defaults = task_defaults

            # we can't create class dynamically because of python2/3
            # __name__ is not overridable

            task_cls = DynamicFuncAsTask
        except Exception as ex:
            logger.error(
                "Failed to create task %s: %s\n%s\n",
                item.__name__,
                str(ex),
                user_side_code(context=5),
                exc_info=show_exc_info(ex),
            )
            raise

        if func_spec.is_class:
            callable_item = six.add_metaclass(_DecoratedUserClassMeta)(item)
        else:
            callable_item = _decorated_user_func(task_cls=task_cls)
        task_cls._callable_item = callable_item

        callable_item.func = item
        callable_item.task_cls = task_cls
        callable_item.task = task_cls
        callable_item.t = task_cls
        return task_cls._callable_item

    # simple `@task` decorator, no options were (probably) given.
    if len(decorator_args) == 1 and callable(decorator_args[0]):
        return decorated(decorator_args[0])

    return decorated
