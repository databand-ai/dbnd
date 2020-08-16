import contextlib
import functools
import logging
import typing

from typing import Type

import six

from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.configuration.environ_config import is_databand_enabled
from dbnd._core.constants import (
    DbndTargetOperationStatus,
    DbndTargetOperationType,
    TaskRunState,
)
from dbnd._core.current import current_task_run, get_databand_run, try_get_current_task
from dbnd._core.decorator.decorated_task import (
    DecoratedPipelineTask,
    DecoratedPythonTask,
    _DecoratedTask,
)
from dbnd._core.decorator.dynamic_tasks import (
    _handle_dynamic_error,
    create_dynamic_task,
)
from dbnd._core.decorator.func_task_call import FuncCall
from dbnd._core.decorator.schemed_result import FuncResultParameter
from dbnd._core.decorator.task_decorator_spec import build_task_decorator_spec
from dbnd._core.errors import show_exc_info
from dbnd._core.errors.errors_utils import log_exception, user_side_code
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task.task import Task
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.utils.timezone import utcnow
from targets import InMemoryTarget
from targets.values import get_value_type_of_obj


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


class DbndFuncProxy(object):
    def __call__(self, *args, **kwargs):
        if _is_tracking_mode():
            with self.tracking_context(args, kwargs) as rt:
                return rt(self.task_func(*args, **kwargs))

        return self.task_cls._call_handler(
            call_user_code=self.task_func, call_args=args, call_kwargs=kwargs
        )

    def __init__(self, task_cls):
        self.task_cls = task_cls  # type: Type[Task]
        # this will make class look like a origin function
        self.task_func = self.task_cls._conf__decorator_spec.item
        functools.update_wrapper(self, self.task_func)
        self._call_count = 0
        self._call_as_func = False
        self._max_call_count = get_dbnd_project_config().max_calls_per_run

    def _call_count_limit_exceeded(self):
        if not self._call_as_func:
            self._call_count += 1
            if self._call_count > self._max_call_count:
                logger.info(
                    "Reached maximum tracking limit of {} tasks. Running function regularly.".format(
                        self._max_call_count
                    )
                )
                self._call_as_func = True
        return self._call_as_func

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

    @contextlib.contextmanager
    def tracking_context(self, call_args, call_kwargs):
        func_call = FuncCall(
            task_cls=self.task_cls,
            call_user_code=self.task_func,
            call_args=tuple(call_args),  # prevent original call_args modification
            call_kwargs=dict(call_kwargs),  # prevent original call_kwargs modification
        )

        # this will be returned by context manager to store function result
        # for later tracking
        def result_tracker(result):
            func_call.result = result
            return result

        user_code_called = False  # whether we got to executing of user code
        user_code_finished = False  # whether we passed executing of user code
        try:
            if not self._call_count_limit_exceeded() and _get_or_create_inplace_task():
                task_run = _create_dynamic_task_run(func_call)
                with task_run.runner.task_run_execution_context(handle_sigterm=True):
                    task_run.set_task_run_state(state=TaskRunState.RUNNING)

                    _log_inputs(task_run)

                    # if we reached this line, then all tracking initialization is
                    # finished successfully, and we're going to execute user code
                    user_code_called = True

                    try:
                        # tracking_context is context manager - user code will run on yield
                        yield result_tracker

                        # if we reached this line, this means that user code finished
                        # successfully without any exceptions
                        user_code_finished = True
                    except Exception as ex:
                        task_run.finished_time = utcnow()

                        error = TaskRunError.build_from_ex(ex, task_run)
                        task_run.set_task_run_state(TaskRunState.FAILED, error=error)
                        raise
                    else:
                        task_run.finished_time = utcnow()

                        # func_call.result should contain result, log it
                        _log_result(task_run, func_call.result)

                        task_run.set_task_run_state(TaskRunState.SUCCESS)
        except Exception:
            if user_code_called and not user_code_finished:
                # if we started to call the user code and not got to user_code_finished
                # line - it means there was user code exception - so just re-raise it
                raise
            # else it's either we didn't reached calling user code, or already passed it
            # then it's some dbnd tracking error - just log it
            _handle_dynamic_error("tracking-init", func_call)

        # if we didn't reached user_code_called=True line - there was an error during
        # dbnd tracking initialization, so nothing is done - user function wasn't called yet
        if not user_code_called:
            # tracking_context is context manager - user code will run on yield
            yield result_tracker


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


def _is_tracking_mode():
    return get_dbnd_project_config().is_tracking_mode()


def _get_or_create_inplace_task():
    """
    try to get existing task, and if not exists - try to get/create inplace_task_run
    """
    current_task = try_get_current_task()
    if not current_task:
        from dbnd._core.inplace_run.inplace_run_manager import try_get_inplace_task_run

        inplace_task_run = try_get_inplace_task_run()
        if inplace_task_run:
            current_task = inplace_task_run.task
    return current_task


def _create_dynamic_task_run(func_call):
    task = create_dynamic_task(func_call)
    dbnd_run = get_databand_run()
    task_run = dbnd_run.create_dynamic_task_run(
        task, task_engine=current_task_run().task_engine
    )
    return task_run


def _log_inputs(task_run):
    """
    For tracking mode. Logs InMemoryTarget inputs.
    """
    try:
        params = task_run.task._params
        for param in params.get_params(input_only=True):
            value = params.get_value(param.name)

            # we
            if isinstance(value, InMemoryTarget):
                try:
                    task_run.tracker.log_target(
                        parameter=param,
                        target=value,
                        value=value._obj,
                        operation_type=DbndTargetOperationType.read,
                        operation_status=DbndTargetOperationStatus.OK,
                    )
                except Exception as ex:
                    log_exception(
                        "Failed to log input param to tracking store.",
                        ex=ex,
                        non_critical=True,
                    )
    except Exception as ex:
        log_exception(
            "Failed to log input params to tracking store.", ex=ex, non_critical=True
        )


def _log_result(task_run, result):
    """
    For tracking mode. Logs the task result and adds it to the target_origin map to support relationships between
    dynamic tasks.
    """
    try:
        task_result_parameter = task_run.task._params.get_param("result")

        # spread result into relevant fields.
        if isinstance(task_result_parameter, FuncResultParameter):
            # assign all returned values to relevant band Outputs
            if result is None:
                return
            for r_name, value in task_result_parameter.named_results(result):
                _log_parameter_value(
                    task_run,
                    parameter_definition=task_run.task._params.get_param(r_name),
                    target=task_run.task._params.get_value(r_name),
                    value=value,
                )
        else:
            _log_parameter_value(
                task_run,
                parameter_definition=task_result_parameter,
                target=task_run.task.result,
                value=result,
            )
    except Exception as ex:
        log_exception(
            "Failed to log result to tracking store.", ex=ex, non_critical=True
        )


def _log_parameter_value(task_run, parameter_definition, target, value):
    try:
        # case what if result is Proxy
        value_type = get_value_type_of_obj(value, parameter_definition.value_type)
        task_run.run.target_origin.add(target, value, value_type)
    except Exception as ex:
        log_exception(
            "Failed to register result to target tracking.", ex=ex, non_critical=True
        )

    try:
        task_run.tracker.log_parameter_data(
            parameter=task_run.task.task_definition.task_class.result,
            target=target,
            value=value,
            operation_type=DbndTargetOperationType.write,  # is it write? (or log?)
            operation_status=DbndTargetOperationStatus.OK,
        )
    except Exception as ex:
        log_exception(
            "Failed to log result to tracking store.", ex=ex, non_critical=True
        )


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

            # we can't create class dynamically because of python2/3
            # __name__ is not overridable

            task_cls = TaskMetaclass(
                str(item.__name__),
                (task_type,),
                dict(
                    _conf__decorator_spec=func_spec,
                    _callable_item=None,
                    __doc__=item.__doc__,
                    __module__=item.__module__,
                    defaults=task_defaults,
                ),
            )
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
            callable_item = DbndFuncProxy(task_cls=task_cls)
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
