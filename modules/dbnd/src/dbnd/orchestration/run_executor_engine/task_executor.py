# © Copyright Databand.ai, an IBM Company 2022

import typing

from typing import List

from dbnd._core.run.run_ctrl import RunCtrl


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd.orchestration.run_executor.run_executor import RunExecutor
    from dbnd.orchestration.run_settings import EngineConfig


class RunExecutorEngine(RunCtrl):
    def __init__(
        self, run_executor, task_executor_type, host_engine, target_engine, task_runs
    ):
        # type: (RunExecutor, str, EngineConfig, EngineConfig,  List[TaskRun])->None
        super(RunExecutorEngine, self).__init__(run=run_executor.run)
        self.run_executor = run_executor
        self.task_executor_type = task_executor_type
        self.host_engine = host_engine
        self.target_engine = target_engine
        self.task_runs = task_runs

    def do_run(self):
        raise NotImplementedError()
