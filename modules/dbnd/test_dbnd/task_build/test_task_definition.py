import logging

from dbnd import Task, task
from dbnd._core.task_build.task_definition import TaskDefinition


logger = logging.getLogger(__name__)


class TestSignature(object):
    def test_task_definition_as_func(self):
        @task
        def a():
            pass

        td = TaskDefinition(a.task, {})
        assert td

    def test_task_definition_as_class(self):
        class TdTask(Task):
            pass

        td = TaskDefinition(TdTask, {})
        assert td
