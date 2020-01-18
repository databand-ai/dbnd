import logging

from typing import Dict

import pytest
import six

from dbnd import PipelineTask, output, parameter
from dbnd._core.errors import DatabandRunError
from dbnd.tasks import PythonTask
from dbnd.testing.helpers_pytest import assert_run_task
from targets import Target
from test_dbnd.factories import TTask, TTaskWithInput


logger = logging.getLogger(__name__)


class TestTaskInputs(object):
    def test_task_input_simple(self, file_on_disk):
        task = TTaskWithInput(t_input=file_on_disk.path)
        assert_run_task(task)

    def test_task_input_via_band1(self, file_on_disk):
        class TTAskWithInputTask1(PipelineTask):
            t_output = output.data

            def band(self):
                self.t_output = TTaskWithInput(t_input=file_on_disk.path)

        assert_run_task(TTAskWithInputTask1())

    def test_task_input_via_band2(self):
        class A(TTask):
            pass

        class TTaskWithInputTask2(PipelineTask):
            x = parameter[str]
            a_output = output.data

            def band(self):
                self.a_output = A(task_name="A_simple")

        c_pipeline = TTaskWithInputTask2(x="some_x_values")
        assert_run_task(c_pipeline)

    def test_input_task(self):
        t = TTaskWithInput(t_input=TTask())
        assert_run_task(t)

    # def test_input_is_missing_task(self):
    #    we don't check for task inputs before they run,
    #    so if TTask is enabled we will fail with an error on read only..
    #     t = TTaskWithInput(t_input=TTask(task_enabled=False))
    #     assert_run_task(t)

    def test_input_is_missing_file(self):
        with pytest.raises(DatabandRunError, match="Airflow executor has failed"):
            t = TTaskWithInput(t_input="file_that_not_exists")
            assert_run_task(t)

    def test_inject_dict(self):
        class TTaskCombineInputs(PythonTask):
            t_inputs = parameter[Dict[int, Target]]
            t_output = output

            def run(self):
                with self.t_output.open("w") as fp:
                    for t_name, t_target in six.iteritems(self.t_inputs):
                        fp.write(t_target.read())

        class TMultipleInjectPipeline(PipelineTask):
            t_types = parameter.value([1, 2])
            t_output = output

            def band(self):
                t_inputs = {t: TTask(t_param=t).t_output for t in self.t_types}
                self.t_output = TTaskCombineInputs(t_inputs=t_inputs).t_output

        task = TMultipleInjectPipeline()
        assert_run_task(task)
        logger.error(task.t_output.read())
