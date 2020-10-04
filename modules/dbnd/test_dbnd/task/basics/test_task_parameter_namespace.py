from typing import List, Tuple

from dbnd import config, dbnd_run_cmd, parameter
from dbnd_test_scenarios.test_common.task.factories import TTask


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
