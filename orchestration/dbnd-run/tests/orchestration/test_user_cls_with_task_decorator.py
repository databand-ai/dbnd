# © Copyright Databand.ai, an IBM Company 2022

import logging
import pickle

from typing import Dict, List

import pytest

from dbnd import output, parameter, pipeline, task
from dbnd._core.errors import DatabandError
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd.testing.orchestration_utils import TargetTestBase
from targets import target
from targets.types import PathStr


logger = logging.getLogger(__name__)


class TError(Exception):
    pass


@task
class ClsAsTask(object):
    def __init__(self, a=6):
        self.a = a

    def run(self):
        assert self.a == 6
        return self.a


@task
class MyAction(object):
    def __init__(self, my_output=output[object], a=1, b=2):
        self.a = a
        self.b = b
        self.my_output = my_output

    def run(self):
        logger.warning(
            "Running Action: %s, %s, output goes to %s"
            % (self.a, self.b, self.my_output)
        )
        self.my_output.write("some output value")
        return self.a


@pipeline
def my_actions_pipeline(a=None):
    step_1 = MyAction(b=101).my_output
    step_2 = MyAction(b=step_1)
    return step_2


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


class TestUserClassWithTaskDecorator(TargetTestBase):
    def test_task_band_user_class(self):
        @task
        class t_1(object):
            def __init__(self, extra_output=output[PathStr]):
                self.extra_output = extra_output

            def run(self):
                target(self.extra_output).mkdir_parent()
                open(self.extra_output, "w").write("")
                return ""

        @pipeline
        def t_pipe():
            v = t_1().extra_output
            return v

        assert_run_task(t_pipe.task())

    def test_simple_defaults(self):
        assert ClsAsTask(6).run() == 6
        ClsAsTask.task().dbnd_run()

    def test_class_super(self):
        class FCls(object):
            def __init__(self, a=6):
                super(FCls, self).__init__()
                self.a = a
                self.b = a

        @task
        class TFCls(FCls):
            def __init__(self, a=6):
                super(TFCls, self).__init__()
                self.a = a

            def run(self):
                assert self.b == self.a
                return self.a

        assert TFCls(6).run() == 6
        assert_run_task(TFCls.t())

    def test_simple_no_call(self):
        @task()
        class TFCls_call(object):
            def __init__(self, a=5):
                self.a = a

            def run(self):
                assert self.a == 6
                return self.a

        TFCls_call(a=6)
        assert_run_task(TFCls_call.t(a=6))

    def test_pipeline(self):
        @task
        class TFCls_input(object):
            def __init__(self, a=5):
                self.a = a

            def run(self):
                assert self.a == 6
                return self.a

        @pipeline
        def my_actions_pipeline(a):
            step_1 = TFCls_input(a=a)
            step_2 = TFCls_input(a=step_1)
            return step_2

        my_actions_pipeline.dbnd_run(a=6)

    def test_input_inplace_output(self):
        @task
        class TFCls_output(object):
            def __init__(self, a=5, output_file=output[PathStr]):
                self.a = a
                self.output_file = output_file

            def run(self):
                assert self.a == 6
                target(self.output_file).mkdir_parent()
                with open(self.output_file, "w") as fp:
                    fp.write("test")
                return self.a

        TFCls_output.task(a=6).dbnd_run()

    def test_user_decorated_class_serializable(self):
        t = ClsAsTask()
        pickled = pickle.dumps(t)
        assert t.a == pickle.loads(pickled).a


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


class TestUserClassInlineCalls(TargetTestBase):
    def test_simple_inline(self, target_1_2):
        assert_run_task(
            ParentCallClsDecoratedTask.t(param_dict={"a": 1}, param_str="p_str")
        )

    def test_error_at_inline(self, target_1_2):
        with pytest.raises(DatabandError):
            ParentCallClsDecoratedTask.dbnd_run(param_dict={"a": 1}, param_str="error")


if __name__ == "__main__":
    # MyAction.task(b=100).dbnd_run()
    #
    # # direct call ( without databand)
    # a = MyAction(b=2)
    # a.run()

    # runs native function
    # my_actions_pipeline()
    # # creates task
    # with new_dbnd_context(conf={"task_namne_custom": {"task_key": "task_value"}}):
    #     t = my_actions_pipeline.task()
    #
    # # creates task
    # with new_dbnd_context(conf={"task_namne_custom": {"task_key": "task_value"}}):
    #     t = my_actions_pipeline.task()

    # final example
    my_actions_pipeline.dbnd_run(
        task_version="now",
        override={
            MyAction.task.b: 3,
            # "MyAction.b": 3
        },
    )
    #
    # t = my_actions_pipeline.task()
    # # creates and runs task
    # my_actions_pipeline.dbnd_run(task_version="now")
