import logging

from dbnd._core.constants import TaskType
from dbnd._core.errors.friendly_error.task_execution import (
    failed_to_assign_result,
    failed_to_process_non_empty_result,
)
from dbnd._core.task.pipeline_task import PipelineTask
from dbnd._core.task.python_task import PythonTask
from dbnd._core.task.task import Task
from dbnd._core.task_build.task_results import FuncResultParameter


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
