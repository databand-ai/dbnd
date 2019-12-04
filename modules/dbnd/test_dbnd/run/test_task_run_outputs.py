import logging

import pandas as pd

import dbnd._core.task_run.task_run

from dbnd import output, task
from dbnd._core.current import get_databand_context
from dbnd._core.run.databand_run import new_databand_run
from dbnd.testing.helpers import initialized_run
from targets import FileTarget
from test_dbnd.factories import TTask


logger = logging.getLogger(__name__)


@task(result=output.hdf5)
def t_f_hdf5(i=1):
    # type:(int)->pd.DataFrame
    return pd.DataFrame(
        data=list(zip(["Bob", "Jessica"], [968, 155])), columns=["Names", "Births"]
    )


class TestTaskRunOutputs(object):
    def test_inconsistent_output(self, monkeypatch):
        from dbnd._core.task_run.task_run_runner import TaskRunRunner

        task = TTask()
        with initialized_run(task):
            runner = task.current_task_run.runner

            with monkeypatch.context() as m:
                m.setattr(FileTarget, "exist_after_write_consistent", lambda a: False)
                m.setattr(FileTarget, "exists", lambda a: False)
                m.setattr(
                    dbnd._core.task_run.task_run_runner,
                    "EVENTUAL_CONSISTENCY_MAX_SLEEPS",
                    1,
                )
                assert not runner.wait_for_consistency()

    def test_hdf5_output(self):
        t_f_hdf5.dbnd_run()
