# © Copyright Databand.ai, an IBM Company 2022

import contextlib
import logging
import typing

from typing import Any, Dict, Tuple

import attr

from dbnd import ParameterDefinition
from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.constants import (
    RESULT_PARAM,
    DbndTargetOperationStatus,
    DbndTargetOperationType,
    TaskRunState,
)
from dbnd._core.current import (
    current_task_run,
    get_databand_run,
    is_verbose,
    try_get_current_task,
)
from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.log.external_exception_logging import log_exception_to_server
from dbnd._core.parameter.parameter_value import ParameterFilters
from dbnd._core.settings import TrackingConfig
from dbnd._core.task.tracking_task import TrackingTask
from dbnd._core.task_build.task_context import try_get_current_task
from dbnd._core.task_build.task_definition import TaskDefinition
from dbnd._core.task_build.task_results import FuncResultParameter
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.utils.callable_spec import args_to_kwargs
from dbnd._core.utils.timezone import utcnow
from targets import InMemoryTarget, Target
from targets.values import get_value_type_of_obj


if typing.TYPE_CHECKING:
    from dbnd._core.task_build.task_decorator import TaskDecorator

logger = logging.getLogger(__name__)


@attr.s
class TrackedFuncCallWithResult(object):
    call_args = attr.ib()  # type:  Tuple[Any]
    call_kwargs = attr.ib()  # type:  Dict[str,Any]
    callable = attr.ib()
    result = attr.ib(default=None)

    def set_result(self, value):
        self.result = value
        return value

    def invoke(self):
        func = self.callable
        return func(*self.call_args, **self.call_kwargs)


class CallableTrackingManager(object):
    def __init__(self, task_decorator):
        # type: (CallableTrackingManager, TaskDecorator) -> None
        self.task_decorator = task_decorator

        self._tracking_task_definition = None
        self._call_count = 0
        self._call_as_func = False
        self._max_call_count = get_dbnd_project_config().max_calls_per_run

    @property
    def callable(self):
        return self.task_decorator.class_or_func

    def get_tracking_task_definition(self):
        if not self._tracking_task_definition:
            self._tracking_task_definition = self._build_tracking_task_definition()
        return self._tracking_task_definition

    def _build_tracking_task_definition(self):
        return TaskDefinition.from_task_decorator(task_decorator=self.task_decorator)

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

    @contextlib.contextmanager
    def tracking_context(self, call_args, call_kwargs):
        user_code_called = False  # whether we got to executing of user code
        user_code_finished = False  # whether we passed executing of user code
        func_call = None
        try:
            # 1. check that we don't have too many calls
            if self._call_count_limit_exceeded():
                yield _do_nothing_decorator
                return

            # 2. Start or reuse existing "main tracking task" that is root for tracked tasks
            if not try_get_current_task():
                """
                try to get existing task, and if not exists - try to get/create inplace_task_run
                """
                from dbnd._core.tracking.script_tracking_manager import (
                    try_get_inplace_tracking_task_run,
                )

                inplace_tacking_task = try_get_inplace_tracking_task_run()
                if not inplace_tacking_task:
                    # we didn't manage to start inplace tracking task run, we will not be able to track
                    yield _do_nothing_decorator
                    return

            tracking_task_definition = self.get_tracking_task_definition()
            callable_spec = tracking_task_definition.task_decorator.get_callable_spec()

            func_call = TrackedFuncCallWithResult(
                callable=self.callable,
                call_args=tuple(call_args),  # prevent original call_args modification
                call_kwargs=dict(call_kwargs),  # prevent original kwargs modification
            )
            # replace any position argument with kwarg if it possible
            args, kwargs = args_to_kwargs(
                callable_spec.args, func_call.call_args, func_call.call_kwargs
            )

            # instantiate inline task
            task = TrackingTask.for_func(tracking_task_definition, args, kwargs)

            # update upstream/downstream relations - needed for correct tracking
            # we can have the task as upstream , as it was executed already
            parent_task = current_task_run().task
            if not parent_task.task_dag.has_upstream(task):
                parent_task.set_upstream(task)

            # checking if any of the inputs are the outputs of previous task.
            # we can add that task as upstream.
            dbnd_run = get_databand_run()
            call_kwargs_as_targets = dbnd_run.target_origin.get_for_map(kwargs)
            for value_origin in call_kwargs_as_targets.values():
                up_task = value_origin.origin_target.task
                task.set_upstream(up_task)

            # creating task_run as a task we found mid-run
            task_run = dbnd_run.create_task_run_at_execution_time(
                task, task_engine=current_task_run().task_engine
            )

            should_capture_log = (
                TrackingConfig.from_databand_context().capture_tracking_log
            )
            with task_run.runner.task_run_execution_context(
                handle_sigterm=True, capture_log=should_capture_log
            ):
                task_run.set_task_run_state(state=TaskRunState.RUNNING)

                _log_inputs(task_run)

                # if we reached this line, then all tracking initialization is
                # finished successfully, and we're going to execute user code
                user_code_called = True

                try:
                    # tracking_context is context manager - user code will run on yield
                    yield func_call.set_result

                    # if we reached this line, this means that user code finished
                    # successfully without any exceptions
                    user_code_finished = True
                # We catch BaseException since we want to catch KeyboardInterrupts as well
                except BaseException as ex:
                    task_run.finished_time = utcnow()

                    error = TaskRunError.build_from_ex(ex, task_run)
                    task_run.set_task_run_state(TaskRunState.FAILED, error=error)
                    raise

                else:
                    task_run.finished_time = utcnow()

                    # func_call.result should contain result, log it
                    _log_result(task_run, func_call.result)

                    task_run.set_task_run_state(TaskRunState.SUCCESS)
        except BaseException:
            if user_code_called and not user_code_finished:
                # if we started to call the user code and not got to user_code_finished
                # line - it means there was user code exception - so just re-raise it
                raise
            # else it's either we didn't reached calling user code, or already passed it
            # then it's some dbnd tracking error - just log it
            if func_call:
                _handle_tracking_error("tracking-init", func_call)
            else:
                log_exception_to_server()
        # if we didn't reached user_code_called=True line - there was an error during
        # dbnd tracking initialization, so nothing is done - user function wasn't called yet
        if not user_code_called:
            # tracking_context is context manager - user code will run on yield
            yield _do_nothing_decorator
            return


def _handle_tracking_error(msg, func_call=None):
    log_exception_to_server()
    location = " for %s" % func_call.callable if func_call else ""
    msg = "Failed during dbnd %s for %s, ignoring, and continue without tracking" % (
        msg,
        location,
    )
    if is_verbose():
        logger.warning(msg, exc_info=True)
    else:
        logger.info(msg)


def _do_nothing_decorator(f):
    return f


def _log_inputs(task_run):
    """
    For tracking mode. Logs InMemoryTarget inputs.
    """
    try:
        params = task_run.task._params
        for param_value in params.get_param_values(ParameterFilters.INPUTS):
            param, value = param_value.parameter, param_value.value

            if isinstance(param_value, InMemoryTarget):
                try:
                    param = param.update_value_meta_conf_from_runtime_value(
                        value, task_run.run.context.settings.tracking
                    )

                    task_run.tracker.log_parameter_data(
                        parameter=param,
                        target=param_value,
                        value=value,
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
    # type: (TaskRun, Any) -> None
    """
    For tracking mode. Logs the task result and adds it to the target_origin map to support relationships between
    dynamic tasks.
    """
    try:
        result_param = task_run.task.task_params.get_param_value(RESULT_PARAM)
        if not result_param:
            logger.debug(
                "No result params to log for task {}".format(task_run.task_af_id)
            )
            return

        # we now the parameter value is a target because this is an output param
        # the target is created in the task creation
        result_param_def, result_target = result_param.parameter, result_param.value

        # spread result into relevant fields.
        if isinstance(result_param_def, FuncResultParameter):
            # assign all returned values to relevant band Outputs
            if result is None:
                return

            for result_name, value in result_param_def.named_results(result):
                # we now the parameter value is a target because this is an output param
                # the target is created in the task creation
                parameter_value = task_run.task.task_params.get_param_value(result_name)

                _log_parameter_value(
                    task_run,
                    parameter_definition=parameter_value.parameter,
                    target=parameter_value.value,
                    value=value,
                )

        else:
            _log_parameter_value(
                task_run,
                parameter_definition=result_param_def,
                target=result_target,
                value=result,
            )

    except Exception as ex:
        log_exception(
            "Failed to log result to tracking store.", ex=ex, non_critical=True
        )


def _log_parameter_value(task_run, parameter_definition, target, value):
    # type: (TaskRun, ParameterDefinition, Target, Any) -> None
    # make sure it will be logged correctly
    parameter_definition = (
        parameter_definition.update_value_meta_conf_from_runtime_value(
            value, task_run.run.context.settings.tracking
        )
    )

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
            parameter=parameter_definition,
            target=target,
            value=value,
            operation_type=DbndTargetOperationType.write,  # is it write? (or log?)
            operation_status=DbndTargetOperationStatus.OK,
        )
    except Exception as ex:
        log_exception(
            "Failed to log result to tracking store.", ex=ex, non_critical=True
        )
