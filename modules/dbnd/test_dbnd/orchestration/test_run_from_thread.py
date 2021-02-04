import threading

from threading import Thread

from dbnd import PythonTask, new_dbnd_context, output
from dbnd._core.context.bootstrap import _dbnd_exception_handling
from dbnd._core.settings import RunConfig
from dbnd_test_scenarios.test_common.task.factories import TTask, ttask_simple


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
    def test_external_task_cmd_line(self):
        with new_dbnd_context(conf={RunConfig.task_executor_type: "local"}):

            def run():
                ttask_simple.dbnd_run()

            main_thread = Thread(target=run)
            main_thread.start()
            main_thread.join()
            t = ttask_simple.task()
            assert t._complete()

    def test_thread_safe_signal_handling(self, capsys):
        _run_in_thread(_dbnd_exception_handling)

    def test_task_run(self):
        t = TTask()
        _run_in_thread(t.dbnd_run)
