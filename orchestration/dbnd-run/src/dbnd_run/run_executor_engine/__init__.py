# © Copyright Databand.ai, an IBM Company 2022
import typing

from dbnd._core.run.run_ctrl import RunCtrl


if typing.TYPE_CHECKING:
    from typing import List

    from dbnd._core.task_run.task_run import TaskRun
    from dbnd_run.run_executor.run_executor import RunExecutor
    from dbnd_run.run_settings import EngineConfig


class RunExecutorEngine(RunCtrl):
    def __init__(
        self,
        run_executor: "RunExecutor",
        task_executor_type: str,
        host_engine: "EngineConfig",
        target_engine: "EngineConfig",
        task_runs: "List[TaskRun]",
    ):
        super(RunExecutorEngine, self).__init__(run=run_executor.run)
        self.run_executor = run_executor
        self.task_executor_type = task_executor_type
        self.host_engine = host_engine
        self.target_engine = target_engine
        self.task_runs = task_runs

    def do_run(self):
        raise NotImplementedError()
