# © Copyright Databand.ai, an IBM Company 2022

import contextlib
import logging
import typing

from abc import ABCMeta, abstractmethod
from itertools import chain

import six

from dbnd._core.current import get_databand_run
from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.log.logging_utils import TaskContextFilter
from dbnd._core.task_build.task_context import task_context
from dbnd._core.utils.basics.nested_context import nested


if typing.TYPE_CHECKING:
    from typing import Any, Dict, Optional

    from dbnd._core.context.databand_context import DatabandContext
    from dbnd._core.parameter.parameter_value import Parameters
    from dbnd._core.settings import DatabandSettings
    from dbnd._core.task.base_task import _BaseTask
    from dbnd._core.task_ctrl.task_dag import _TaskDagNode
    from dbnd._core.task_run.task_run import TaskRun

logger = logging.getLogger(__name__)


class TaskSubCtrl(object):
    def __init__(self, task):
        super(TaskSubCtrl, self).__init__()

        self.task = task  # type: _BaseTask

    @property
    def dbnd_context(self):
        # type: (TaskSubCtrl) -> DatabandContext
        return self.task.dbnd_context

    @property
    def ctrl(self):
        # type: (TaskSubCtrl) -> _BaseTaskCtrl
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
    def params(self):  # type: () -> Parameters
        return self.task._params

    @property
    def settings(self):  # type: ()-> DatabandSettings
        return self.task.settings

    def get_task_by_task_id(self, task_id):
        return self.dbnd_context.task_instance_cache.get_task_by_id(task_id)

    def __repr__(self):
        return "%s.%s" % (self.task.task_id, self.__class__.__name__)


@six.add_metaclass(ABCMeta)
class _BaseTaskCtrl(TaskSubCtrl):
    """
    task.ctrl
    Main dispatcher for task sub-actions
    1. used by Tracking via  TrackingTaskCtrl
    """

    def __init__(self, task):
        super(_BaseTaskCtrl, self).__init__(task)

        from dbnd._core.task_ctrl.task_dag import _TaskDagNode  # noqa: F811
        from dbnd._core.task_ctrl.task_descendant import (
            TaskDescendants as _TaskDescendants,
        )

        self._task_dag = _TaskDagNode(task)
        self.descendants = _TaskDescendants(task)

        # will be assigned by the latest Run
        self.last_task_run = None  # type: Optional[TaskRun]
        self.force_task_run_uid = None  # force task run uid

    def _initialize_task(self):
        return

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

    def banner(self, msg, color=None, task_run=None):
        try:
            from dbnd._core.task_ctrl.task_visualiser import _TaskBannerBuilder

            builder = _TaskBannerBuilder(task=self.task, msg=msg, color=color)

            return builder.build_tracking_banner(task_run=task_run)

        except Exception as ex:
            log_exception(
                "Failed to calculate banner for '%s'" % self.task_id,
                ex,
                non_critical=True,
            )
            return msg + (" ( task_id=%s)" % self.task_id)
