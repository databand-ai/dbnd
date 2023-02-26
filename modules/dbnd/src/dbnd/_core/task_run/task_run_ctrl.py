# © Copyright Databand.ai, an IBM Company 2022

import typing

from dbnd._core.task_ctrl.task_ctrl import TaskSubCtrl


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun


class TaskRunCtrl(TaskSubCtrl):
    def __init__(self, task_run):
        # type: (TaskRun)-> None
        super(TaskRunCtrl, self).__init__(task=task_run.task)
        self.run = task_run.run
        self.task_run = task_run

        self.job = task_run  # backward compatible to old code

    @property
    def context(self):
        return self.run.context

    @property
    def task_run_uid(self):
        return self.task_run.task_run_uid

    @property
    def task_run_attempt_uid(self):
        return self.task_run.task_run_attempt_uid
