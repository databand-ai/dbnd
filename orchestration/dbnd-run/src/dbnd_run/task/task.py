# © Copyright Databand.ai, an IBM Company 2022
import contextlib
import datetime
import logging
import random
import typing
import warnings

from contextlib import contextmanager
from typing import Any, Dict, Optional

from dbnd._core.configuration.dbnd_config import config as dbnd_config
from dbnd._core.constants import (
    DbndTargetOperationStatus,
    DbndTargetOperationType,
    OutputMode,
    ParamValidation,
    TaskEssence,
    _TaskDbndRun,
    _TaskParamContainer,
)
from dbnd._core.current import get_databand_run, try_get_current_task_run
from dbnd._core.errors.friendly_error.task_build import incomplete_output_found_for_task
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.log.logging_utils import TaskContextFilter
from dbnd._core.parameter.constants import ParameterScope
from dbnd._core.parameter.parameter_builder import output, parameter
from dbnd._core.parameter.parameter_definition import ParameterDefinition
from dbnd._core.parameter.parameter_value import ParameterFilters
from dbnd._core.task.task_mixin import _TaskCtrlMixin
from dbnd._core.task.task_with_params import _TaskWithParams
from dbnd._core.task_build.task_context import task_context
from dbnd._core.task_ctrl.task_ctrl import _BaseTaskCtrl
from dbnd._core.utils.basics.nested_context import nested
from dbnd._core.utils.basics.nothing import NOTHING
from dbnd._core.utils.traversing import flatten, traverse_to_str
from dbnd_run import errors
from dbnd_run.run_executor_engine.local_task_executor import topological_sort
from dbnd_run.run_settings import RunSettings
from dbnd_run.run_settings.env import EnvConfig
from dbnd_run.task_ctrl.task_output_builder import calculate_path
from targets import InMemoryTarget, target
from targets.target_config import TargetConfig, folder
from targets.values import get_value_type_of_obj
from targets.values.version_value import VersionStr


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.task_build.task_cls__call_state import TaskCallState
    from dbnd._core.task_ctrl.task_dag import _TaskDagNode
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd_run.task_ctrl.task_run_executor import TaskRunExecutor

DEFAULT_CLASS_VERSION = ""

logger = logging.getLogger(__name__)

system_passthrough_param = parameter.system(scope=ParameterScope.children)


class Task(_TaskWithParams, _TaskCtrlMixin, _TaskParamContainer, _TaskDbndRun):
    """
    This is the base class of all dbnd Tasks, the base unit of work in databand.

    A dbnd Task describes a unit or work.

    A ``run`` method must be present in a subclass

    Each ``parameter`` of the Task should be declared as members::

        class MyTask(dbnd.Task):
            count = dbnd.parameter[int]
            second_param = dbnd.parameter[str]
    """

    _conf_confirm_on_kill_msg = None  # get user confirmation on task kill if not empty
    _conf__require_run_dump_file = False

    _task_band_result = output(default=None, system=True)
    _meta_output = output(
        system=True,
        output_name="meta",
        output_ext="",
        target_config=folder,
        significant=False,
        description="Location of all internal outputs (e.g. metrics)",
    )
    task_band = output.json(output_name="band", system=True)

    task_enabled = system_passthrough_param(default=True)[bool]
    task_enabled_in_prod = system_passthrough_param(default=True)[bool]
    validate_no_extra_params = ParamValidation.error

    # for permanent bump of task version use Task.task_class_version
    task_version = parameter(
        default="1",
        description="task version, directly affects task signature ",
        scope=ParameterScope.children,
    )[VersionStr]

    task_class_version = parameter.value(
        default=DEFAULT_CLASS_VERSION,
        system=True,
        description="task code version, "
        "use while you want persistent change in your task version",
    )

    task_env = parameter.value(
        default="local",
        description="task environment name",
        scope=ParameterScope.children,
    )[EnvConfig]

    task_target_date = parameter(
        default="today",
        description="task data target date",
        scope=ParameterScope.children,
    )[datetime.date]

    task_airflow_op_kwargs = parameter.system(
        default=None, description="airflow operator kwargs"
    )[Dict[str, object]]

    task_config = parameter.system(empty_default=True)[Dict]
    task_is_system = parameter.system(default=False)[bool]

    task_in_memory_outputs = system_passthrough_param(
        default=False, description="Store all task outputs in memory"
    )[bool]

    task_output_path_format = system_passthrough_param(
        default=None, description="Format string used to generate task output paths"
    )[str]

    task_is_dynamic = system_passthrough_param(
        default=False,
        scope=ParameterScope.children,
        description="task was executed from within another task",
    )[bool]

    # for example: if task.run doesn't have access to databand, we can't run runtime tasks
    task_supports_dynamic_tasks = parameter.system(
        default=True, description="indicates if task can run dynamic databand tasks"
    )[bool]

    task_retries = parameter.system(
        default=0,
        description="Total number of attempts to run the task. So task_retries=3 -> task can fail 3 times before we give up",
    )[int]

    task_retry_delay = parameter.system(
        default="15s",
        description="timedelta to wait before retrying a task. Example: 5s",
    )[datetime.timedelta]

    task_essence = TaskEssence.ORCHESTRATION

    def __init__(self, **kwargs):
        super(Task, self).__init__(**kwargs)

        # used to communicate return value of "user function"
        self._dbnd_call_state = None  # type: Optional[TaskCallState]
        self.ctrl = TaskCtrl(self)

    def band(self):
        """
        Please, do not override this function only in Pipeline/External tasks!

        We do all wiring work in Meta classes only.
        Our implementation should never be coupled to code!
        """
        return

    def run(self):
        """
        The task run method, to be overridden in a subclass.

        See :ref:`Task.run`
        """
        pass  # default impl

    @property
    def task_outputs(self):
        """
        The output that this Task produces.

        The output of the Task determines if the Task needs to be run--the task
        is considered finished iff the outputs all exist.
        """
        return self.ctrl.relations.task_outputs_user

    @property
    def task_dag(self):
        # type: (...)->_TaskDagNode
        return self.ctrl.task_dag

    @property
    def descendants(self):
        return self.ctrl.descendants

    def _complete(self):
        """
        If the task has any outputs, return ``True`` if all outputs exist. Otherwise, return ``False``.

        However, you may freely override this method with custom logic.
        """
        # we check only user side task outputs
        # all system tasks outputs are not important (if the exists or not)
        # user don't see them
        outputs = [
            o for o in flatten(self.task_outputs) if not o.config.overwrite_target
        ]
        if len(outputs) == 0:
            if not self.task_band:
                warnings.warn(
                    "Task %r without outputs has no custom complete() and no task band!"
                    % self,
                    stacklevel=2,
                )
                return False
            else:
                return self.task_band.exists()

        incomplete_outputs = [str(o) for o in outputs if not o.exists()]

        num_of_incomplete_outputs = len(incomplete_outputs)

        if 0 < num_of_incomplete_outputs < len(outputs):
            complete_outputs = [str(o) for o in outputs if o.exists()]
            exc = incomplete_output_found_for_task(
                self.task_name, complete_outputs, incomplete_outputs
            )

            if self.run_settings.run.validate_task_outputs_on_build:
                raise exc
            else:
                logger.warning(str(exc))

        return num_of_incomplete_outputs == 0

    @property
    def current_task_run(self):
        # type: ()->TaskRun
        return get_databand_run().get_task_run(self.task_id)

    @property
    def current_task_run_executor(self):
        # type: ()->TaskRunExecutor
        return self.current_task_run.task_run_executor

    @property
    def run_settings(self) -> RunSettings:
        return self.dbnd_context.run_settings

    def _output(self):
        """
        The default output that this Task produces.

        Use outputs! Override only if you are writing "base" class.
        """
        return NOTHING

    def _requires(self):
        """
        Override in "template" tasks which themselves are supposed to be subclassed.

        Must return an iterable which, among others, contains the _requires() of
        the superclass.
        """

    def _task_submit(self):
        """Task submission logic, by default we just call -> ``_task_run()`` -> ``run()``."""
        return self._task_run()

    def _task_run(self):
        # bring all relevant files
        task_run_executor = self.current_task_run.task_run_executor
        task_run_executor.sync_local.sync_pre_execute()
        param_values = self.task_params.get_param_values()

        with auto_load_save_params(
            task=self, auto_read=self._conf_auto_read_params, param_values=param_values
        ):
            result = self.run()

        task_run_executor.sync_local.sync_post_execute()
        # publish all relevant files
        return result

    @property
    def tracker(self):
        return self.current_task_run.tracker

    @property
    def metrics(self):
        # backward compatible code
        return self.tracker

    def get_template_vars(self):
        # TODO: move to cached version, (after relations are built)
        base = {
            "task": self,
            "task_family": self.task_family,
            "task_name": self.task_name,
            "task_signature": self.task_signature,
            "task_id": self.task_id,
        }
        base.update(self._params.get_params_serialized(ParameterFilters.INPUTS))
        if self.task_target_date is None:
            base["task_target_date"] = "input"
        return base

    def on_kill(self):
        """
        Override this method to cleanup subprocesses when a task instance gets killed.

        Any use of the threading, subprocess or multiprocessing
        module within an operator needs to be cleaned up or it will leave
        ghost processes behind.
        """

    def _get_task_output_path_format(self, output_mode):
        """
        Defines the format string used to generate all task outputs.

        For example:
           {root}/{env_label}/{task_target_date}/{task_name}/{task_name}{task_class_version}_{task_signature}/{output_name}{output_ext}
        """
        if self.task_output_path_format:
            # explicit input - first priority
            return self.task_output_path_format
        if self._conf__base_output_path_fmt:
            # from class definition
            return self._conf__base_output_path_fmt

        # default behaviour
        if self.task_env.production and output_mode == OutputMode.prod_immutable:
            return self.run_settings.output.path_prod_immutable_task
        return self.run_settings.output.path_task

    def get_target(self, name, config=None, output_ext=None, output_mode=None):
        name = name or "tmp/dbnd-tmp-%09d" % random.randint(0, 999999999)
        config = config or TargetConfig()
        path_pattern = self._get_task_output_path_format(output_mode)

        path = calculate_path(
            task=self,
            name=name,
            output_ext=output_ext,
            is_dir=config.folder,
            path_pattern=path_pattern,
        )

        return target(path, config=config)

    def get_root(self):
        return self.task_env.root

    def _initialize(self):
        super(Task, self)._initialize()
        self.ctrl._initialize_task()

    def _should_run(self):
        if not self.task_enabled:
            return False

        if self.task_env.production:
            return self.task_enabled_in_prod or self.settings.run.enable_prod

        return True

    def _save_param(self, parameter, original_value, current_value):
        # type: (ParameterDefinition, Any, Any) -> None
        # it's output! we are going to save it.
        # task run doesn't always exist
        task_run = try_get_current_task_run()
        access_status = DbndTargetOperationStatus.OK
        try:
            if isinstance(original_value, InMemoryTarget):
                parameter.value_type = get_value_type_of_obj(
                    current_value, parameter.value_type
                )

            parameter.dump_to_target(original_value, current_value)
            # it's a workaround, we don't want to change parameter for outputs (dynamically)
            # however, we need proper value type to "dump" preview an other meta.
            # we will update it only for In memory targets only for now

        except Exception as ex:
            access_status = DbndTargetOperationStatus.NOK
            raise errors.task_execution.failed_to_save_value_to_target(
                ex, self, parameter, original_value, current_value
            )
        finally:
            if task_run:
                try:
                    task_run.tracker.log_parameter_data(
                        parameter=parameter,
                        target=original_value,
                        value=current_value,
                        operation_type=DbndTargetOperationType.write,
                        operation_status=access_status,
                    )
                except Exception as ex:
                    logger.warning("Failed to log target to tracking store. %s", ex)

    @dbnd_handle_errors(exit_on_error=False)
    def dbnd_run(self):
        # type: (...)-> DatabandRun
        """Run task via Databand execution system."""
        # this code should be executed under context!
        from dbnd._core.current import get_databand_context

        ctx = get_databand_context()
        run = ctx.dbnd_run_task(self)
        return run


@contextmanager
def auto_load_save_params(task, auto_read, param_values):
    original_values = [(p, p.value) for p in param_values]

    # keep all outputs that were "read" during pre-read phase
    # we will not save this values on post-execute phase
    outputs_as_simple_objects = {}

    if auto_read:
        # in .run() we are going to pre-fetch all values
        # * we do it eager loading for all parameters (not lazy)

        for param_value in param_values:
            runtime_value = param_value.parameter.calc_runtime_value(
                param_value.value, task=task
            )
            if param_value.value is not runtime_value:  # only if different
                if param_value.parameter.is_output():
                    # outputs() are going to be processed as well,
                    # we might change them to Path from Target
                    # however, we don't need to consider this value  after that
                    outputs_as_simple_objects[param_value.name] = runtime_value
                param_value.update_param_value(runtime_value)

    try:
        yield original_values

        # we are at the .__exit__ of run, let's save all changed fields
        # no exception, still not good as atomic commit for all files, but less "garbage" left in case of failure
        for param_value, original_value in original_values:
            # TODO: implement Atomic commit
            if not param_value.parameter.is_output():
                continue
            current_value = param_value.value
            if current_value is original_value:
                # nothing to do original_value is the same
                continue
            param_name = param_value.name
            if (
                param_name not in outputs_as_simple_objects
                or outputs_as_simple_objects.get(param_value.name) is not current_value
            ):
                # only if it's user assigned value
                # otherwise we will save Path/PathStr object to the output
                task._save_param(param_value.parameter, original_value, current_value)
            param_value.update_param_value(
                original_value
            )  # do it , so we know what we have processed
    finally:
        # we always want to revert values on .run() exit ( all read and outputs)
        for param_value, original_value in original_values:
            if param_value.value is not original_value:  # only if different
                param_value.update_param_value(original_value)


Task.task_definition.hidden = True

TASK_PARAMS_COUNT = len(
    [
        v
        for k, v in Task.__dict__.items()
        if isinstance(v, ParameterDefinition) and v.value_type_str != "Target"
    ]
)


class TaskCtrl(_BaseTaskCtrl):
    def __init__(self, task):
        super(TaskCtrl, self).__init__(task)

        self.task: Task = task
        from dbnd_run.task_ctrl.task_dag_describe import DescribeDagCtrl
        from dbnd_run.task_ctrl.task_relations import TaskRelations  # noqa: F811
        from dbnd_run.task_ctrl.task_repr import TaskCallRepresentation

        self._should_run = self.task._should_run()
        self._relations = TaskRelations(task)
        self.describe_dag = DescribeDagCtrl(task)
        self.task_repr = TaskCallRepresentation(self.task)

    @property
    def relations(self):  # type: () -> TaskRelations
        return self._relations

    @property
    def task_env(self):
        # type: ()-> EnvConfig
        return self.task.task_env

    def _initialize_task(self):
        # target driven relations are relevant only for orchestration tasks
        self.relations.initialize_relations()

        self.task_dag.initialize_dag_node()

        self.task_repr.initialize()

        super(TaskCtrl, self)._initialize_task()

        # validate circle dependencies
        # may be we should move it to global level because of performance issues
        # however, by running it at every task we'll be able to find the code that causes the issue
        # and show it to user
        if self.task.run_settings.output.recheck_circle_dependencies:
            tasks = self.task_dag.subdag_tasks()
            topological_sort(tasks)

    def should_run(self):
        # convert to property one day
        return self._should_run

    def subdag_tasks(self):
        return self.task_dag.subdag_tasks()

    def save_task_band(self):
        if self.task.task_band:
            task_outputs = traverse_to_str(self.task.task_outputs)
            self.task.task_band.as_object.write_json(task_outputs)

    @contextlib.contextmanager
    def task_context(self, phase):
        # we don't want logs/user wrappers at this stage
        with nested(
            task_context(self.task, phase),
            TaskContextFilter.task_context(self.task.task_id),
            dbnd_config.config_layer_context(self.task.task_config_layer),
        ):
            yield

    @property
    def task_inputs(self):
        return self.relations.task_inputs

    @property
    def task_outputs(self):
        return self.relations.task_outputs

    def banner(self, msg, color=None, task_run=None, verbose=False, exc_info=None):
        from dbnd_run.task_ctrl.task_visualizer import _TaskOrchestrationBannerBuilder

        builder = _TaskOrchestrationBannerBuilder(
            task=self.task,
            msg=msg,
            color=color,
            verbose=verbose,
            print_task_band=self.dbnd_context.run_settings.describe.print_task_band,
        )

        return builder.build_orchestration_banner(task_run=task_run, exc_info=exc_info)
