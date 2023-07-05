# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.constants import TaskType
from dbnd._core.current import current_task_run
from dbnd._core.task_build.task_context import (
    TaskContextPhase,
    current_phase,
    try_get_current_task,
)
from dbnd._core.task_build.task_results import FuncResultParameter
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.utils.callable_spec import args_to_kwargs
from dbnd_run.errors.task_execution import (
    failed_to_assign_result,
    failed_to_process_non_empty_result,
)
from dbnd_run.task.pipeline_task import PipelineTask
from dbnd_run.task.python_task import PythonTask
from dbnd_run.task.task import Task
from targets.inline_target import InlineTarget


logger = logging.getLogger(__name__)


class _DecoratedCallableTask(Task):
    _dbnd_decorated_task = True

    # default result for any decorated function
    result = None

    def _invoke_func(self, extra_kwargs=None, force_invoke=False):
        # this function is in charge of calling user defined code (decorated function) call
        # usually it's called from from task.run/task.band
        extra_kwargs = extra_kwargs or {}
        spec = self.task_decorator.get_callable_spec()
        invoke_kwargs = {}
        for name in spec.args:
            # if there is no parameter - it was disabled at TaskDefinition building stage
            if self._params.get_param(name) is None:
                continue
            invoke_kwargs[name] = getattr(self, name)

        invoke_kwargs.update(extra_kwargs)

        if not self._dbnd_call_state:
            from dbnd._core.task_build.task_cls__call_state import TaskCallState

            self._dbnd_call_state = TaskCallState()
        self._dbnd_call_state.start()

        if self.task_decorator.is_class:
            # this is the case of
            # @task
            # class UserClass:
            #     pass
            # now we are in the Task instance, it was created via UserClass() at @pipeline
            obj_cls = self.task_decorator.class_or_func
            invoke_kwargs["__call_original_cls"] = True
            self.task_user_obj = obj_cls(**invoke_kwargs)
            result = self.task_user_obj.run()
        else:
            # we are going to run user function
            func_call = spec.item
            result = func_call(**invoke_kwargs)

        self._dbnd_call_state.finish(result)

        result_param = self.__class__.result
        if result_param is None and result:
            raise failed_to_process_non_empty_result(self, result)

        if isinstance(result_param, FuncResultParameter):
            # if we have result that combined from different output params
            # assign all returned values to relevant outputs
            # so they will be automatically saved
            if result is None:
                raise failed_to_assign_result(self, result_param)
            result_param.validate_result(result)
            for r_name, value in result_param.named_results(result):
                setattr(self, r_name, value)
        else:
            self.result = result
        return result

    def on_kill(self):
        task_user_obj = getattr(self, "task_user_obj", None)
        if task_user_obj is not None and hasattr(task_user_obj, "on_kill"):
            task_user_obj.on_kill()
            return
        else:
            super(_DecoratedCallableTask, self).on_kill()


class DecoratedPythonTask(PythonTask, _DecoratedCallableTask):
    _conf__task_type_name = TaskType.python

    def run(self):
        self._invoke_func(force_invoke=True)


class DecoratedPipelineTask(PipelineTask, _DecoratedCallableTask):
    _conf__task_type_name = TaskType.pipeline

    def band(self):
        return self._invoke_func()


def call_decorated_callable_task_with_dbnd_run(
    self: "TaskDecorator", call_args, call_kwargs
):
    # we are at orchestration mode
    task_cls = self.get_task_cls()

    current = try_get_current_task()
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

        from dbnd_run.current import try_get_run_executor

        run_executor = try_get_run_executor()
        if not run_executor:
            raise Exception("Run Executor is not activated")
        # if possible we will run it as "orchestration" task
        # with parameters parsing
        if (
            run_executor.run_config.task_run_at_execution_time_enabled
            and current.task_supports_dynamic_tasks
        ):
            return _run_task_from_another_task_execution(
                self,
                run_executor=run_executor,
                parent_task=current,
                call_args=call_args,
                call_kwargs=call_kwargs,
            )
        # we can not call it in "dbnd" way, fallback to normal call
        if self.is_class:
            call_kwargs["__call_original_cls"] = False
        return self.class_or_func(*call_args, **call_kwargs)
    else:
        raise Exception()


def _run_task_from_another_task_execution(
    self: "TaskDecorator", run_executor, parent_task: Task, call_args, call_kwargs
) -> TaskRun:
    # task is running from another task
    task_cls = self.get_task_cls()

    from dbnd import PipelineTask, pipeline
    from dbnd._core.task_build.dbnd_decorator import _default_output

    # orig_call_args, orig_call_kwargs = call_args, call_kwargs
    call_args, call_kwargs = args_to_kwargs(
        self.get_callable_spec().args, call_args, call_kwargs
    )

    # Map all kwargs to the "original" target of that objects
    # for example: for DataFrame we'll try to find a relevant target that were used to read it
    # get all possible value's targets
    call_kwargs_as_targets = run_executor.run.target_origin.get_for_map(call_kwargs)
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
        run_executor.run_config.task_run_at_execution_time_in_memory_outputs,
    )

    if issubclass(task_cls, PipelineTask):
        # if it's pipeline - create new databand run
        # create override _task_default_result to be object instead of target
        task_cls = pipeline(
            self.class_or_func, _task_default_result=_default_output
        ).task_cls

        # instantiate new inline execution
        task = task_cls(*call_args, **call_kwargs)
        # if it's pipeline - create new databand run
        run = run_executor.databand_context.dbnd_run_task(task)
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
            parent_task_run = current_task_run()
            task_run = run_executor.run_task_at_execution_time(
                task, task_engine=parent_task_run.task_run_executor.task_engine
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
