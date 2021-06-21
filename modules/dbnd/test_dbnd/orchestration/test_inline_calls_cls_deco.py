from typing import Dict, List

from pytest import fixture

from dbnd import output, parameter, task
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase


class TError(Exception):
    pass


@task(result=(output(name="datasets")[List[str]]))
class InlineCallClsDecoratedTask(object):
    def __init__(self, param_dict=parameter[Dict], param_str=parameter[str]):
        self.param_dict = param_dict
        self.param_str = param_str

    def run(self):
        assert self.param_dict
        assert self.param_str
        if self.param_str == "error":
            raise TError("Raising as requested")
        self.datasets = list(self.param_dict.keys())


@task(result=(output(name="datasets")[List[str]]))
class ParentCallClsDecoratedTask:
    def __init__(self, param_dict=parameter[str], param_str=parameter[str]):
        self.param_dict = param_dict
        self.param_str = param_str

    def run(self):
        assert self.param_dict
        assert self.param_str
        # MODE A - current
        # too implicit - magic
        self.datasets = InlineCallClsDecoratedTask(
            param_dict=self.param_dict, param_str=self.param_str
        )


class TestInlineDecoClsCalls(TargetTestBase):
    @fixture
    def target_1_2(self):
        t = self.target("file.txt")
        t.as_object.writelines(["1", "2"])
        return t

    def test_simple_inline(self, target_1_2):
        assert_run_task(
            ParentCallClsDecoratedTask.t(param_dict={"a": 1}, param_str="p_str")
        )

    def test_error_at_inline(self, target_1_2):
        assert_run_task(
            ParentCallClsDecoratedTask.t(param_dict={"a": 1}, param_str="error")
        )
