from threading import Thread

from dbnd import new_dbnd_context
from dbnd._core.settings import RunConfig
from test_dbnd.factories import ttask_simple


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
