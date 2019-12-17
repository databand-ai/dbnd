import contextlib
import logging
import typing

from typing import List

from dbnd._core.run.run_ctrl import RunCtrl


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun
    from dbnd._core.run.databand_run import DatabandRun
    from dbnd._core.settings import EngineConfig


logger = logging.getLogger(__name__)


class TaskExecutor(RunCtrl):
    def __init__(self, run, host_engine, target_engine, task_runs):
        # type: (DatabandRun, EngineConfig,  List[TaskRun])->None
        super(TaskExecutor, self).__init__(run=run)
        self.run = run
        self.host_engine = host_engine
        self.target_engine = target_engine
        self.task_runs = task_runs
        self.task_executor_type = host_engine.task_executor_type

    @contextlib.contextmanager
    def prepare_run(self):
        yield

    def do_run(self):
        raise NotImplementedError()
