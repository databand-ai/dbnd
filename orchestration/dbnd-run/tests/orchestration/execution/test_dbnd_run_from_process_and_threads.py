# © Copyright Databand.ai, an IBM Company 2022

import logging
import sys
import threading

from threading import Thread

from dbnd import PythonTask, new_dbnd_context, output
from dbnd._core.utils.basics.signal_utils import register_graceful_sigterm
from dbnd.orchestration.run_settings import RunConfig
from dbnd.tasks.basics import SimplestTask
from dbnd.testing.helpers import run_dbnd_subprocess
from dbnd_test_scenarios.test_common.task.factories import TTask, ttask_simple


logger = logging.getLogger(__name__)

CURRENT_PY_FILE = __file__.replace(".pyc", ".py")


class TMissingOutputs(PythonTask):
    some_output = output
    forgotten_output = output

    def run(self):
        self.some_output.write("")


def _run_in_thread(target):
    x = threading.Thread(target=target)
    x.start()
    x.join()


class TestRunFromThread(object):
    def test_thread_external_task_cmd_line(self):
        with new_dbnd_context(conf={RunConfig.task_executor_type: "local"}):

            def run():
                ttask_simple.dbnd_run()

            main_thread = Thread(target=run)
            main_thread.start()
            main_thread.join()
            t = ttask_simple.task()
            assert t._complete()

    def test_thread_safe_signal_handling(self, capsys):
        _run_in_thread(register_graceful_sigterm)

    def test_thread_task_run(self):
        t = TTask()
        _run_in_thread(t.dbnd_run)

    def test_subprocess_inplace_run(self):
        run_dbnd_subprocess([sys.executable, CURRENT_PY_FILE, "new_dbnd_context"])


if __name__ == "__main__":
    SimplestTask(task_env="local").dbnd_run()
