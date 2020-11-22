from typing import List

from dbnd import PipelineTask, Task, output, parameter
from dbnd.testing.helpers_pytest import assert_run_task
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase


class ResultTask(Task):
    t_id = parameter[int].default(0)
    result = output[str]

    def run(self):
        self.result = str(self.t_id)


class ConsumerTask(Task):
    p = parameter[List[str]]

    def run(self):
        assert self.p == ["0", "1", "2"]


class Pipeline(PipelineTask):
    def band(self):
        ConsumerTask(p=[ResultTask(t_id=i).result for i in range(3)])


class TestReturnValue(TargetTestBase):
    def test_class_pipe(self):
        assert_run_task(Pipeline())
