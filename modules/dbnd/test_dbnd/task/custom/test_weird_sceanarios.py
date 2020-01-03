import logging

from dbnd import PipelineTask, data, output, parameter
from dbnd.testing.helpers_pytest import assert_run_task
from test_dbnd.factories import TTask


logger = logging.getLogger(__name__)


class SomeObject(object):
    def __init__(self, value):
        self.value = value


class PipelineWithObjectParameter(PipelineTask):
    t_output = output

    def band(self):
        self.t_output = TTask(t_param=SomeObject(1)).t_output


class TestWeirdScenarios(object):
    def test_object_parameters_simple(self):
        assert_run_task(PipelineWithObjectParameter())

    def test_object_parameters_immutable(self):
        assert (
            TTask(t_param=SomeObject(1)).task_id != TTask(t_param=SomeObject(2)).task_id
        )
