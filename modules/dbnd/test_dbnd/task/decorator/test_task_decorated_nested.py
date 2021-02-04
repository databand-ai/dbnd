import logging

from dbnd import output, pipeline, task
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase
from targets import target
from targets.types import PathStr


logger = logging.getLogger(__name__)


@task
class InnerTask(object):
    def __init__(self, name):
        self.name = name

    def run(self):
        return self.name


@task
class OuterTask(object):
    def __init__(self, name):
        self.name = name

    def run(self):
        inners = []
        for i in range(3):
            inner = InnerTask(name="{} {}".format(self.name, i))
            inners.append(inner)
        return inners


@pipeline
def class_pipe():
    return OuterTask(name="dodo")


@task
def inner(name):
    return name * 2


@task
def outer(name):
    return inner(name[:2])


@pipeline
def func_pipe():
    return outer("dbnd rule!!")


class TestNestedTasks(TargetTestBase):
    def test_class_pipe(self):
        assert_run_task(class_pipe.task())

    def test_func_pipe(self):
        assert_run_task(func_pipe.task())
