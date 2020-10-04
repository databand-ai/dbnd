import threading

from dbnd import PythonTask, output
from dbnd._core.context.bootstrap import _dbnd_exception_handling
from dbnd_test_scenarios.test_common.task.factories import TTask


class TMissingOutputs(PythonTask):
    some_output = output
    forgotten_output = output

    def run(self):
        self.some_output.write("")


def _run_in_thread(target):
    x = threading.Thread(target=target)
    x.start()
    x.join()


class TestThreadSafe(object):
    def test_thread_safe_signal_handling(self, capsys):
        _run_in_thread(_dbnd_exception_handling)

    def test_task_run(self):
        t = TTask()
        _run_in_thread(t.dbnd_run)
