import logging

from dbnd.tasks.basics import SimplestTask


logger = logging.getLogger(__name__)


class TestTaskFromTaskBand(object):
    def test_simple_task(self):
        t = SimplestTask(simplest_param=1)
        t.dbnd_run()

        t2 = SimplestTask(simplest_param=2)
        assert str(t2.simplest_output) != str(t.simplest_output)

        t3 = SimplestTask(task_band=t.task_band)

        assert str(t3.simplest_output) == str(t.simplest_output)
