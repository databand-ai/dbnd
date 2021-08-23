from typing import List, Tuple

import dbnd

from dbnd import auto_namespace, config, dbnd_run_cmd, namespace, parameter
from dbnd_test_scenarios.test_common.task.factories import TTask


class TestTaskNamespace(object):
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


class TestParameterNamespaceTask(object):
    def testWithNamespaceConfig(self):
        class A(TTask):
            task_namespace = "mynamespace"
            p = parameter[int]

        with config({"mynamespace.A": {"p": "999"}}):
            assert 999 == A().p

    def testWithNamespaceCli(self):
        class A(TTask):
            task_namespace = "mynamespace"
            p1 = parameter.value(100)
            expected = parameter[int]

            def complete(self):
                if self.p1 != self.expected:
                    raise ValueError
                return True

        assert dbnd_run_cmd("mynamespace.A -r expected=100")
        assert dbnd_run_cmd("mynamespace.A -r p1=200 -r expected=200")

    def testListWithNamespaceCli(self):
        class A(TTask):
            task_namespace = "mynamespace"
            l_param = parameter.value([1, 2, 3])
            expected = parameter[List[int]]

            def complete(self):
                if self.l_param != self.expected:
                    raise ValueError
                return True

        assert dbnd_run_cmd("mynamespace.A -r expected=[1,2,3]")
        assert dbnd_run_cmd("mynamespace.A -r l_param=[1,2,3] -r expected=[1,2,3]")

    def testTupleWithNamespaceCli(self):
        class A(TTask):
            task_namespace = "mynamespace"
            t = parameter.value(((1, 2), (3, 4)))
            expected = parameter[Tuple]

            def complete(self):
                if self.t != self.expected:
                    raise ValueError
                return True

        assert dbnd_run_cmd("mynamespace.A -r expected=((1,2),(3,4))")
        assert dbnd_run_cmd(
            "mynamespace.A -r t=((1,2),(3,4)) -r expected=((1,2),(3,4))"
        )
