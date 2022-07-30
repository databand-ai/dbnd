# © Copyright Databand.ai, an IBM Company 2022

import logging

from pytest import fixture

from dbnd import Task, task
from dbnd._core.task_build.task_definition import TaskDefinition


logger = logging.getLogger(__name__)


@fixture
def databand_test_context():
    # override,
    # so we don't have all these logic running on setup phase
    pass


class TestTaskDefinition(object):
    def test_task_definition_as_func(self):
        @task
        def a():
            pass

        td = TaskDefinition.from_task_cls(a.task, {})
        assert td

    def test_task_definition_as_class(self):
        class TdTask(Task):
            pass

        td = TaskDefinition.from_task_cls(TdTask, {})
        assert td
