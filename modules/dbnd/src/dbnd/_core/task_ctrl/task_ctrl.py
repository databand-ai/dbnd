import contextlib
import logging
import typing

from abc import ABCMeta, abstractmethod
from itertools import chain

import six

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.current import get_databand_run
from dbnd._core.log.logging_utils import TaskContextFilter
from dbnd._core.task_build.task_context import task_context
from dbnd._core.utils.basics.nested_context import nested
from dbnd._core.utils.traversing import traverse_to_str


if typing.TYPE_CHECKING:
    from typing import Optional, Dict, Any
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd._core.settings import EnvConfig, DatabandSettings
    from dbnd._core.parameter.parameter_value import Parameters
    from dbnd._core.task_ctrl.task_relations import TaskRelations
    from dbnd._core.task_ctrl.task_dag import _TaskDagNode
    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.task_ctrl.task_validator import TaskValidator
    from dbnd._core.task_ctrl.task_descendant import TaskDescendants

logger = logging.getLogger(__name__)


class TaskSubCtrl(object):
    def __init__(self, task):
        super(TaskSubCtrl, self).__init__()

        from dbnd._core.task.task import Task

        self.task = task  # type: Task

    @property
    def dbnd_context(self):
        # type: (TaskSubCtrl) -> DatabandContext
        return self.task.dbnd_context

    @property
    def ctrl(self):
        # type: (TaskSubCtrl) -> TaskCtrl
        return self.task.ctrl

    @property
    def task_id(self):
        # type: (TaskSubCtrl) -> str
        return self.task.task_id

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
    def params(self):  # type: () -> Parameters
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


@six.add_metaclass(ABCMeta)
class _BaseTaskCtrl(TaskSubCtrl):
    def __init__(self, task):
        super(_BaseTaskCtrl, self).__init__(task)

        from dbnd._core.task_ctrl.task_dag import _TaskDagNode  # noqa: F811
        from dbnd._core.task_ctrl.task_visualiser import TaskVisualiser  # noqa: F811
        from dbnd._core.task_ctrl.task_dag_describe import DescribeDagCtrl
        from dbnd._core.task_ctrl.task_descendant import TaskDescendants
        from dbnd._core.task_ctrl.task_repr import TaskRepr

        self._task_dag = _TaskDagNode(task)
        self.descendants = TaskDescendants(task)

        self._visualiser = TaskVisualiser(task)
        self.describe_dag = DescribeDagCtrl(task)

        self.task_repr = TaskRepr(self.task)

        # will be assigned by the latest Run
        self.last_task_run = None  # type: Optional[TaskRun]
        self.force_task_run_uid = None  # force task run uid

    def _initialize_task(self):
        # only at the end we can build the final version of "function call"
        self.task_repr.initialize()

    def banner(self, msg, color=None, task_run=None):
        return self.visualiser.banner(msg=msg, color=color, task_run=task_run)

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
        ):
            yield

    @abstractmethod
    def should_run(self):
        pass

    def io_params(self):
        return chain(self.task_outputs.values(), self.task_inputs.values())

    @property
    @abstractmethod
    def task_inputs(self):
        # type: () -> Dict[str, Dict[str][Any]]
        raise NotImplementedError()

    @property
    @abstractmethod
    def task_outputs(self):
        # type: () -> Dict[str, Dict[str][Any]]
        raise NotImplementedError()


class TaskCtrl(_BaseTaskCtrl):
    def __init__(self, task):
        from dbnd._core.task.task import Task

        assert isinstance(task, Task)

        super(TaskCtrl, self).__init__(task)

        from dbnd._core.task_ctrl.task_relations import TaskRelations  # noqa: F811
        from dbnd._core.task_ctrl.task_validator import TaskValidator

        self._relations = TaskRelations(task)
        self.task_validator = TaskValidator(task)

        self._should_run = self.task._should_run()

    def _initialize_task(self):
        # target driven relations are relevant only for orchestration tasks
        self.relations.initialize_relations()
        self.task_dag.initialize_dag_node()

        super(TaskCtrl, self)._initialize_task()

        # validate circle dependencies
        # may be we should move it to global level because of performance issues
        # however, by running it at every task we'll be able to find the code that causes the issue
        # and show it to user
        if self.dbnd_context.settings.run.recheck_circle_dependencies:
            self.task_dag.topological_sort()

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
            config.config_layer_context(self.task.task_config_layer),
        ):
            yield

    @property
    def task_inputs(self):
        return self.relations.task_inputs

    @property
    def task_outputs(self):
        return self.relations.task_outputs


class TrackingTaskCtrl(_BaseTaskCtrl):
    def __init__(self, task):
        from dbnd._core.task.tracking_task import TrackingTask

        assert isinstance(task, TrackingTask)

        super(TrackingTaskCtrl, self).__init__(task)

    @property
    def task_inputs(self):
        return dict(dict())

    @property
    def task_outputs(self):
        return dict(dict())

    def should_run(self):
        return True
