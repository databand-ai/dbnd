import typing

# © Copyright Databand.ai, an IBM Company 2022
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd_run.run_settings.env import EnvConfig


if typing.TYPE_CHECKING:

    from dbnd._core.task_run.task_run import TaskRun
    from dbnd_run.task.task import Task


class _TaskRunExecutorCtrl(TaskRunCtrl):
    def __init__(self, task_run: "TaskRun"):
        super().__init__(task_run=task_run)
        self.task: Task = task_run.task

    @property
    def run_executor(self):
        return self.run.run_executor

    @property
    def task_run_executor(self):
        return self.task_run.task_run_executor

    @property
    def task_env(self) -> EnvConfig:
        return self.task.task_env
