import datetime
import logging
import random
import typing
import warnings

from typing import Dict

from dbnd._core.constants import OutputMode, _TaskParamContainer
from dbnd._core.current import get_databand_run
from dbnd._core.errors.friendly_error.task_build import incomplete_output_found_for_task
from dbnd._core.failures import dbnd_handle_errors
from dbnd._core.parameter.parameter_builder import output, parameter
from dbnd._core.parameter.parameter_definition import (
    ParameterDefinition,
    ParameterScope,
)
from dbnd._core.settings.env import EnvConfig
from dbnd._core.task.base_task import _BaseTask
from dbnd._core.task_ctrl.task_ctrl import TaskCtrl
from dbnd._core.task_ctrl.task_output_builder import calculate_path
from dbnd._core.utils.basics.nothing import NOTHING
from dbnd._core.utils.traversing import flatten
from targets import target
from targets.target_config import TargetConfig, folder
from targets.value_meta import ValueMetaConf
from targets.values.version_value import VersionStr


if typing.TYPE_CHECKING:
    from dbnd._core.task_ctrl.task_dag import _TaskDagNode
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.decorator.safe_task_call import TaskCallState

DEFAULT_CLASS_VERSION = ""

logger = logging.getLogger(__name__)


class Task(_BaseTask, _TaskParamContainer):
    """
    This is the base class of all dbnd Tasks, the base unit of work in databand.

    A dbnd Task describes a unit or work.

    The key methods of a Task, which must be implemented in a subclass are:

    * :py:meth:`run` - the computation done by this task.

    Each :py:class:`~dbnd.parameter` of the Task should be declared as members:

    .. code:: python

        class MyTask(dbnd.Task):
            count = dbnd.parameter[int]
            second_param = dbnd.parameter[str]

    In addition to any declared properties and methods, there are a few
    non-declared properties, which are created by the :py:class:`TaskMetaclass`
    metaclass:

    """

    """
        This value can be overriden to set the namespace that will be used.
        (See :ref:`Task.namespaces_famlies_and_ids`)
        If it's not specified and you try to read this value anyway, it will return
        garbage. Please use :py:meth:`get_task_namespace` to read the namespace.

        Note that setting this value with ``@property`` will not work, because this
        is a class level value.
    """

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

    task_enabled = parameter.system(scope=ParameterScope.children)[bool]
    task_enabled_in_prod = parameter.system(scope=ParameterScope.children)[bool]

    # for permanent bump of task version use Task.task_class_version
    task_version = parameter(
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
        description="task environment name", scope=ParameterScope.children
    )[EnvConfig]

    task_target_date = parameter(
        description="task data target date", scope=ParameterScope.children
    )[datetime.date]

    task_airflow_op_kwargs = parameter.system(
        default=None, description="airflow operator kwargs"
    )[Dict[str, object]]

    task_config = parameter.system(empty_default=True)[Dict]
    task_is_system = parameter.system(default=False)[bool]

    task_in_memory_outputs = parameter.system(
        scope=ParameterScope.children, description="Store all task outputs in memory"
    )[bool]
    task_is_dynamic = parameter.system(
        scope=ParameterScope.children,
        description="task was executed from within another task",
    )[bool]

    # for example: if task.run doesn't have access to databand, we can't run runtime tasks
    task_supports_dynamic_tasks = parameter.system(
        default=True, description="indicates if task can run dynamic databand tasks"
    )[bool]

    task_retries = parameter.system(
        description="Total number of attempts to run the task. So task_retries=3 -> task can fail 3 times before we give up"
    )[int]

    task_retry_delay = parameter.system(
        description="timedelta to wait before retrying a task. Example: 5s"
    )[datetime.timedelta]

    _dbnd_call_state = None  # type: TaskCallState

    def __init__(self, **kwargs):
        super(Task, self).__init__(**kwargs)
        self.ctrl = TaskCtrl(self)

    def band(self):
        """
        Please, do not override this function only in Pipeline/External tasks! we do all wiring work in Meta classes only
        Our implementation should never be coupled to code!
        :return:
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
        See :ref:`Task.task_outputs`
        """
        return self.ctrl.relations.task_outputs_user

    @property
    def task_dag(self):
        # type: (...)->_TaskDagNode
        return self.ctrl.task_dag

    def _complete(self):
        """
        If the task has any outputs, return ``True`` if all outputs exist.
        Otherwise, return ``False``.

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
                self.task_meta.task_name, complete_outputs, incomplete_outputs
            )

            if self.task_env.settings.run.validate_task_outputs_on_build:
                raise exc
            else:
                logger.warning(str(exc))

        return num_of_incomplete_outputs == 0

    @property
    def current_task_run(self):
        # type: ()->TaskRun
        return get_databand_run().get_task_run(self.task_id)

    def _output(self):
        """
        The default output that this Task produces. Use outputs! Override only if you are writing "base" class
        """
        return NOTHING

    def _requires(self):
        """
        Override in "template" tasks which themselves are supposed to be
        subclassed

        Must return an iterable which among others contains the _requires() of
        the superclass.
        See :ref:`Task.requires`
        """
        pass

    def _task_submit(self):
        """
        Task submission logic, by default we just call -> _task_run() -> run()
        """
        return self._task_run()

    def _task_run(self):
        # bring all relevant files
        self.current_task_run.sync_local.sync_pre_execute()
        with self._auto_load_save_params(
            auto_read=self._conf_auto_read_params, save_on_change=True
        ):
            result = self.run()

        self.current_task_run.sync_local.sync_post_execute()
        # publish all relevant files
        return result

    def set_upstream(self, task_or_task_list):
        self.task_dag.set_upstream(task_or_task_list)

    def set_downstream(self, task_or_task_list):
        self.task_dag.set_downstream(task_or_task_list)

    def __lshift__(self, other):
        return self.set_upstream(other)

    def __rshift__(self, other):
        return self.set_downstream(other)

    def set_global_upstream(self, task_or_task_list):
        self.task_dag.set_global_upstream(task_or_task_list)

    @property
    def metrics(self):
        # backward compatible code
        return self.current_task_run.tracker

    def log_dataframe(
        self,
        key,
        df,
        with_preview=True,
        with_schema=True,
        with_size=True,
        with_stats=False,
    ):
        meta_conf = ValueMetaConf(
            log_preview=with_preview,
            log_schema=with_schema,
            log_size=with_size,
            log_stats=with_stats,
        )
        self.metrics.log_data(key, df, meta_conf=meta_conf)

    def log_metric(self, key, value, source=None):
        """
        Logs the passed-in parameter under the current run, creating a run if necessary.
        :param key: Parameter name (string)
        :param value: Parameter value (string)
        """
        return self.metrics.log_metric(key, value, source=source)

    def log_system_metric(self, key, value):
        """Shortcut for log_metric(..., source="system") """
        return self.log_metric(key, value, source="system")

    def log_artifact(self, name, artifact):
        """Log a local file or directory as an artifact of the currently active run."""
        return self.metrics.log_artifact(name, artifact)

    def get_template_vars(self):
        # TODO: move to cached version, (after relations are built)
        base = {
            "task": self,
            "task_family": self.task_meta.task_family,
            "task_name": self.task_meta.task_name,
            "task_signature": self.task_meta.task_signature,
            "task_id": self.task_meta.task_id,
        }
        base.update(self._params.get_params_serialized(input_only=True))
        if self.task_target_date is None:
            base["task_target_date"] = "input"
        return base

    def on_kill(self):
        """
        Override this method to cleanup subprocesses when a task instance
        gets killed. Any use of the threading, subprocess or multiprocessing
        module within an operator needs to be cleaned up or it will leave
        ghost processes behind.
        """
        pass

    def _get_task_output_path_format(self, output_mode):
        if self.task_env.production and output_mode == OutputMode.prod_immutable:
            return self.settings.output.path_prod_immutable_task
        return self._conf__base_output_path_fmt or self.settings.output.path_task

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

    @dbnd_handle_errors(exit_on_error=False)
    def dbnd_run(self):
        # type: (...)-> DatabandRun
        """
        Run task via Databand execution system
        """
        # this code should be executed under context!
        from dbnd._core.current import get_databand_context

        ctx = get_databand_context()
        result = ctx.dbnd_run_task(self)
        return result


Task.task_definition.hidden = True

TASK_PARAMS_COUNT = len(
    [
        v
        for k, v in Task.__dict__.items()
        if isinstance(v, ParameterDefinition) and v.value_type_str != "Target"
    ]
)
