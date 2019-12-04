import logging

from dbnd import data
from dbnd.tasks.basics import SimplestTask
from dbnd.testing import assert_run_task
from test_dbnd.factories import TTask


logger = logging.getLogger(__name__)


class TestTaskReimplementation(object):
    def test_ttask(self):
        class TTaskImpl(TTask):
            t_input = data

            def run(self):
                pass  # will faile

        bad_task_impl = TTaskImpl(t_input=SimplestTask())

        assert bad_task_impl

        class TTaskImpl(TTask):
            t_input = data
            # should not fail as we don't override input

        task = TTaskImpl(t_input=SimplestTask())
        assert_run_task(task)
        assert task
