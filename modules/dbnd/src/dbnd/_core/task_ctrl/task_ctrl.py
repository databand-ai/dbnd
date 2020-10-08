import contextlib
import logging
import typing

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.current import get_databand_run
from dbnd._core.log.logging_utils import TaskContextFilter
from dbnd._core.task_build.task_context import task_context
from dbnd._core.utils.basics.nested_context import nested
from dbnd._core.utils.traversing import traverse_to_str


if typing.TYPE_CHECKING:
    from typing import Optional
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd._core.settings import EnvConfig, DatabandSettings
    from dbnd._core.task_ctrl.task_meta import TaskMeta
    from dbnd._core.task_ctrl.task_parameters import TaskParameters
    from dbnd._core.task_ctrl.task_relations import TaskRelations
    from dbnd._core.task_ctrl.task_dag import _TaskDagNode
    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.task_ctrl.task_validator import TaskValidator


logger = logging.getLogger(__name__)


class TaskSubCtrl(object):
    def __init__(self, task):
        super(TaskSubCtrl, self).__init__()

        from dbnd._core.task.task import Task

        self.task = task  # type: Task
        self.dbnd_context = self.task_meta.dbnd_context  # type: DatabandContext

    @property
    def ctrl(self):
        # type: (TaskSubCtrl) -> TaskCtrl
        return self.task.ctrl

    @property
    def task_id(self):
        # type: (TaskSubCtrl) -> str
        return self.task.task_id

    @property
    def task_meta(self):
        # type: (TaskSubCtrl) -> TaskMeta
        return self.task.task_meta

    @property
    def task_dag(self):
        # type: (TaskSubCtrl) -> _TaskDagNode
        return self.ctrl._task_dag

    @property
    def visualiser(self):
        return self.ctrl._visualiser

    @property
    def meta_info_serializer(self):
        return self.ctrl._meta_info_serializer

    @property
    def params(self):  # type: () -> TaskParameters
        return self.task._params

    @property
    def settings(self):  # type: ()-> DatabandSettings
        return self.task.settings

    @property
    def relations(self):  # type: () -> TaskRelations
        return self.ctrl._relations

    @property
    def validator(self):  # type: () -> TaskValidator
        return self.ctrl.task_validator

    @property
    def task_env(self):
        # type: ()-> EnvConfig
        return self.task.task_env

    def get_task_by_task_id(self, task_id):
        return self.dbnd_context.task_instance_cache.get_task_by_id(task_id)

    def __repr__(self):
        return "%s.%s" % (self.task.task_id, self.__class__.__name__)


class TaskCtrl(TaskSubCtrl):
    def __init__(self, task):
        super(TaskCtrl, self).__init__(task)

        from dbnd._core.task.task import Task

        assert isinstance(task, Task)
        from dbnd._core.task_ctrl.task_relations import TaskRelations  # noqa: F811
        from dbnd._core.task_ctrl.task_dag import _TaskDagNode  # noqa: F811
        from dbnd._core.task_ctrl.task_visualiser import TaskVisualiser  # noqa: F811
        from dbnd._core.task_ctrl.task_dag_describe import DescribeDagCtrl
        from dbnd._core.task_ctrl.task_validator import TaskValidator

        self._relations = TaskRelations(task)
        self.task_validator = TaskValidator(task)
        self._task_dag = _TaskDagNode(task)

        self._visualiser = TaskVisualiser(task)
        self.describe_dag = DescribeDagCtrl(task)

        self._should_run = self.task_meta.task_enabled and self.task._should_run()

        # will be assigned by the latest Run
        self.last_task_run = None  # type: Optional[TaskRun]
        self.force_task_run_uid = None  # force task run uid

    def banner(self, msg, color=None, task_run=None):
        return self.visualiser.banner(msg=msg, color=color, task_run=task_run)

    def _initialize_task(self):
        self.relations.initialize_relations()
        self.task_dag.initialize_dag_node()

        from dbnd._core.task_ctrl.task_repr import TaskReprBuilder

        tb = TaskReprBuilder(self.task)
        # only at the end we can build the final version of "function call"
        self.task_meta.task_command_line = tb.calculate_command_line_for_task()
        self.task_meta.task_functional_call = tb.calculate_task_call()

        # validate circle dependencies
        # may be we should move it to global level because of performance issues
        # however, by running it at every task we'll be able to find the code that causes the issue
        # and show it to user
        if self.dbnd_context.settings.core.recheck_circle_dependencies:
            self.task_dag.topological_sort()

    def should_run(self):
        # convert to property one day
        return self._should_run

    def subdag_tasks(self):
        return self.task_dag.subdag_tasks()

    @property
    def task_run(self):
        # type: ()-> TaskRun
        run = get_databand_run()
        return run.get_task_run(self.task.task_id)

    @contextlib.contextmanager
    def task_context(self, phase):
        # we don't want logs/user wrappers at this stage
        with nested(
            task_context(self.task, phase),
            TaskContextFilter.task_context(self.task.task_id),
            config.config_layer_context(self.task.task_meta.config_layer),
        ):
            yield

    def save_task_band(self):
        if self.task.task_band:
            task_outputs = traverse_to_str(self.task.task_outputs)
            self.task.task_band.as_object.write_json(task_outputs)
