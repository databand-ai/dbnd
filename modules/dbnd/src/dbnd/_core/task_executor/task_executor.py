# © Copyright Databand.ai, an IBM Company 2022

import typing

from typing import List

from dbnd._core.run.run_ctrl import RunCtrl


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.settings import EngineConfig
    from dbnd._core.task_run.task_run import TaskRun


class TaskExecutor(RunCtrl):
    def __init__(self, run, task_executor_type, host_engine, target_engine, task_runs):
        # type: (DatabandRun, str, EngineConfig, EngineConfig,  List[TaskRun])->None
        super(TaskExecutor, self).__init__(run=run)

        self.task_executor_type = task_executor_type
        self.host_engine = host_engine
        self.target_engine = target_engine
        self.task_runs = task_runs

    def do_run(self):
        raise NotImplementedError()
