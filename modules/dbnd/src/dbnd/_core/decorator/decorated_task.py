import logging

from dbnd._core.configuration import get_dbnd_project_config
from dbnd._core.constants import TaskType
from dbnd._core.decorator.func_task_call import TaskCallState
from dbnd._core.decorator.schemed_result import FuncResultParameter
from dbnd._core.errors.friendly_error.task_execution import (
    failed_to_assign_result,
    failed_to_process_non_empty_result,
)
from dbnd._core.task.pipeline_task import PipelineTask
from dbnd._core.task.python_task import PythonTask
from dbnd._core.task.task import Task


logger = logging.getLogger(__name__)


class _DecoratedTask(Task):
    _dbnd_decorated_task = True
    result = None

    def _invoke_func(self, extra_kwargs=None, force_invoke=False):
        # this function is in charge of calling user defined code (decorated function) call
        # usually it's called from from task.run/task.band
        extra_kwargs = extra_kwargs or {}
        spec = self._conf__decorator_spec
        invoke_kwargs = {}
        for name in spec.args:
            # if there is no parameter - it was disabled at TaskDefinition building stage
            if self._params.get_param(name) is None:
                continue
            invoke_kwargs[name] = getattr(self, name)
        self.task_user_obj = None

        invoke_kwargs.update(extra_kwargs)
        if spec.is_class:
            obj_cls = self._callable_item
            invoke_kwargs["__force_invoke"] = force_invoke
            # we will get to Metaclass  ( same one that created current `self`)
            # this time  we want it to run user code directly
            self.task_user_obj = obj_cls(**invoke_kwargs)
            try:
                setattr(self.task_user_obj, "_dbnd_task", self)
            except Exception:
                pass
            result = self.task_user_obj.run()
        else:
            # we are going to run user function
            if not self._dbnd_call_state:
                self._dbnd_call_state = TaskCallState()
            self._dbnd_call_state.start()
            func_call = spec.item
            result = func_call(**invoke_kwargs)
            self._dbnd_call_state.finish(result)

        result_param = self.__class__.result
        if result_param is None and result:
            raise failed_to_process_non_empty_result(self, result)

        # spread result into relevant fields.
        if isinstance(result_param, FuncResultParameter):
            # assign all returned values to relevant band Outputs
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
            super(_DecoratedTask, self).on_kill()


class DecoratedPythonTask(PythonTask, _DecoratedTask):
    _conf__task_type_name = TaskType.python

    def run(self):
        self._invoke_func(force_invoke=True)


class DecoratedPipelineTask(PipelineTask, _DecoratedTask):
    _conf__task_type_name = TaskType.pipeline

    def band(self):
        return self._invoke_func()
