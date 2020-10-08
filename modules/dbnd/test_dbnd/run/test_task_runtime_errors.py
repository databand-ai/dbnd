import pytest

from pandas import DataFrame

from dbnd import PythonTask, data, new_dbnd_context, output
from dbnd._core.errors import DatabandRunError
from dbnd_test_scenarios.test_common.targets.target_test_base import TargetTestBase


class TMissingOutputs(PythonTask):
    some_output = output
    forgotten_output = output

    def run(self):
        self.some_output.write("")


class TestTaskErrors(TargetTestBase):
    def test_no_outputs(self, capsys):
        with new_dbnd_context(conf={"run": {"task_executor_type": "local"}}):
            with pytest.raises(DatabandRunError, match="Failed tasks are:"):
                TMissingOutputs().dbnd_run()

    def test_to_read_input(self, capsys):
        class TCorruptedInput(PythonTask):
            some_input = data[DataFrame]
            forgotten_output = output

            def run(self):
                self.some_output.write("")

        with new_dbnd_context(conf={"run": {"task_executor_type": "local"}}):
            t = self.target("some_input.json").write("corrupted dataframe")

            with pytest.raises(DatabandRunError, match="Failed tasks are:"):
                TCorruptedInput(some_input=t).dbnd_run()
