# © Copyright Databand.ai, an IBM Company 2022
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl


class _TaskRunExecutorCtrl(TaskRunCtrl):
    @property
    def run_executor(self):
        return self.run.run_executor

    @property
    def task_run_executor(self):
        return self.task_run.task_run_executor
