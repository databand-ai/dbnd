import logging

from dbnd._core.configuration.environ_config import is_databand_enabled
from dbnd._core.constants import TaskType
from dbnd._core.current import current_task_run
from dbnd._core.decorator.dynamic_tasks import run_dynamic_task
from dbnd._core.decorator.schemed_result import FuncResultParameter
from dbnd._core.errors.friendly_error.task_execution import (
    failed_to_assign_result,
    failed_to_process_non_empty_result,
)
from dbnd._core.inplace_run.airflow_dag_inplace_tracking import (
    track_airflow_dag_run_operator_run,
    try_get_airflow_context,
)
from dbnd._core.plugin.dbnd_airflow_operator_plugin import (
    build_task_at_airflow_dag_context,
    is_in_airflow_dag_build_context,
)
from dbnd._core.task.pipeline_task import PipelineTask
from dbnd._core.task.python_task import PythonTask
from dbnd._core.task.task import Task
from dbnd._core.task_build.task_context import (
    TaskContextPhase,
    current_phase,
    current_task,
    has_current_task,
)


logger = logging.getLogger(__name__)


class _DecoratedTask(Task):
    _dbnd_decorated_task = True
    result = None

    @classmethod
    def _call_handler(cls, call_user_code, call_args, call_kwargs):
        """
        -= Use "Step into My Code"" to get back from Databand code! =-

        decorated object call/creation  ( my_func(), MyDecoratedTask()
        """
        force_invoke = call_kwargs.pop("__force_invoke", False)
        if force_invoke or not is_databand_enabled():
            # 1. Databand is not enabled
            # 2. we have this call coming from Task.run / Task.band direct invocation
            return call_user_code(*call_args, **call_kwargs)

        if is_in_airflow_dag_build_context():
            return build_task_at_airflow_dag_context(
                task_cls=cls, call_args=call_args, call_kwargs=call_kwargs
            )

        if not has_current_task():
            ######
            # DBND HANDLING OF CALL
            airflow_task_context = try_get_airflow_context()
            if airflow_task_context:
                return track_airflow_dag_run_operator_run(
                    task_cls=cls,
                    call_args=call_args,
                    call_kwargs=call_kwargs,
                    airflow_task_context=airflow_task_context,
                )
            else:
                # direct call to the function
                return call_user_code(*call_args, **call_kwargs)

        # now we can make some decisions what we do with the call
        # it's not coming from _invoke_func
        # but from   user code ...   some_func()  or SomeTask()
        current = current_task()
        phase = current_phase()
        if phase is TaskContextPhase.BUILD:
            # we are in the @pipeline context, we are building execution plan
            t = cls(*call_args, **call_kwargs)

            # we are in inline debug mode -> we are going to execute the task
            # we are in the band
            # and want to return result of the object
            if t.task_definition.single_result_output:
                return t.result

            # we have multiple outputs ( result, another output.. ) -> just return task object
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
                task_run = run_dynamic_task(
                    parent_task_run=current_task_run(),
                    task_cls=cls,
                    call_args=call_args,
                    call_kwargs=call_kwargs,
                )
                t = task_run.task
                # if we are inside run, we want to have real values, not deferred!
                if t.task_definition.single_result_output:
                    return t.__class__.result.load_from_target(t.result)
                    # we have func without result, just fallback to None
                return t

        # we can not call it in"databand" way, fallback to normal execution
        return call_user_code(*call_args, **call_kwargs)

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
            func_call = spec.item
            result = func_call(**invoke_kwargs)

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
