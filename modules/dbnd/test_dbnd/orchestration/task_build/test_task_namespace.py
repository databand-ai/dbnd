import inspect

import dbnd

from dbnd import auto_namespace, namespace


class TestTaskAutoNamespace(object):
    this_module = __name__

    def test_standard_namespace(self):
        class MyTask(dbnd.Task):
            pass

        assert MyTask.get_task_family() == "MyTask"

    def test_auto_namespace_global(self):
        auto_namespace()

        class MyTask(dbnd.Task):
            pass

        assert MyTask.task_definition.task_family == self.this_module + ".MyTask"

    def test_auto_namespace_scope(self):
        auto_namespace(scope=__name__)
        namespace("bleh", scope="")

        class MyTask(dbnd.Task):
            pass

        assert MyTask.task_definition.task_family == self.this_module + ".MyTask"

    def test_auto_namespace_not_matching(self):
        auto_namespace(scope="incorrect_namespace")
        namespace("bleh", scope="")

        class MyTask(dbnd.Task):
            pass

        namespace(scope="incorrect_namespace")
        namespace(scope="")
        assert MyTask.task_definition.task_family == "bleh.MyTask"

    def test_auto_namespace_not_matching_2(self):
        auto_namespace(scope="incorrect_namespace")

        class MyTask(dbnd.Task):
            pass

        assert MyTask.task_definition.task_family == "MyTask"
