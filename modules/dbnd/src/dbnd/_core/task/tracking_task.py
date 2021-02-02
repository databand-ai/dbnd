import logging
import os
import typing

from dbnd._core.constants import TaskEssence, _TaskParamContainer
from dbnd._core.current import get_databand_run
from dbnd._core.decorator.schemed_result import FuncResultParameter
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.parameter.parameter_definition import (
    ParameterDefinition,
    ParameterScope,
)
from dbnd._core.parameter.parameter_value import ParameterValue
from dbnd._core.settings.env import EnvConfig
from dbnd._core.task.base_task import _BaseTask
from dbnd._core.task.task_mixin import _TaskCtrlMixin
from dbnd._core.task_ctrl.task_ctrl import TrackingTaskCtrl
from dbnd._core.task_ctrl.task_output_builder import windows_drive_re
from dbnd._core.task_ctrl.task_relations import traverse_and_set_target
from dbnd._core.utils.basics.memoized import cached
from dbnd._core.utils.basics.nothing import NOTHING, is_not_defined
from dbnd._core.utils.timezone import utcnow
from targets import target
from targets.target_config import folder
from targets.utils.path import no_trailing_slash


if typing.TYPE_CHECKING:
    from dbnd._core.task_ctrl.task_dag import _TaskDagNode
    from dbnd._core.task_run.task_run import TaskRun

logger = logging.getLogger(__name__)


class TrackingTask(_BaseTask, _TaskCtrlMixin, _TaskParamContainer):
    """
    Task for tracking only use-cases.
    """

    task_essence = TaskEssence.TRACKING

    task_env = parameter.value(
        description="task environment name", scope=ParameterScope.children
    )[EnvConfig]

    def __init__(self, **kwargs):
        super(TrackingTask, self).__init__(**kwargs)
        self.ctrl = TrackingTaskCtrl(self)

        # replace the appropriate parameters in the Task
        self.task_version = utcnow().strftime("%Y%m%d_%H%M%S")
        self.task_target_date = utcnow().date()

        self.task_outputs = dict()
        # used for setting up only parameter that defined at the class level.
        for name, attr in self.__class__.__dict__.items():
            if isinstance(attr, ParameterDefinition):
                param_value = self.task_meta.task_params[name]
                setattr(self, name, param_value.value)

    def _initialize(self):
        super(TrackingTask, self)._initialize()

        in_params = self._params.get_params_serialized(
            significant_only=True, input_only=True
        )
        self.task_meta.initialize_task_id(in_params)

        for p, value in self._params.get_param_values(output_only=True):
            if is_not_defined(value):
                value = p.build_output(task=self)
                setattr(self, p.name, value)

            if isinstance(p, FuncResultParameter):
                continue

            # This is used to keep backward compatibility for tracking luigi behaviour
            # This is not something we want to keep, at least not in this form
            value = traverse_and_set_target(value, p._target_source(self))
            self.task_outputs[p.name] = value

        out_params = self._params.get_param_values(output_only=True)
        self.task_meta.initialize_task_output_id(out_params)

        self.ctrl._initialize_task()

    def _get_param_value(self, param_name):
        if hasattr(self, param_name):
            return getattr(self, param_name)

        param_value = self.task_meta.task_params[param_name]
        if isinstance(param_value, ParameterValue):
            return param_value.value

        return param_value

    @property
    def tracker(self):
        return self.current_task_run.tracker

    @property
    def task_dag(self):
        return self.ctrl.task_dag

    @property
    def descendants(self):
        return self.ctrl.descendants

    #############################
    # BACKWARD COMPATIBLE TO `Task`
    task_class_version = ""
    task_is_dynamic = False
    task_is_system = False

    @property
    def current_task_run(self):
        # type: ()->TaskRun
        return get_databand_run().get_task_run(self.task_id)

    @property
    def task_in_memory_outputs(self):
        return True

    @property
    @cached()
    def _meta_output(self):
        # in some sense this is a duplication of dbnd._core.task_ctrl.task_output_builder.calculate_path
        # but it also breaking an awful abstraction and a lot of inner functions which is good
        task_env = self._params.get_value("task_env")
        sep = "/"
        root = no_trailing_slash(str(task_env.root))
        if windows_drive_re.match(root):
            sep = os.sep
        path = sep.join(
            (
                root,
                task_env.env_label,
                str(self.task_target_date),
                self.task_name,
                self.task_name + "_" + self.task_meta.task_signature,
                "_meta_output",
                "meta",
            )
        )

        # meta_output is directory
        path += sep

        return target(path, folder)

    def _complete(self):
        return None
