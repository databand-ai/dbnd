import pytest

from pandas import DataFrame

from dbnd import PythonTask, data, output
from dbnd._core.errors import DatabandExecutorError
from test_dbnd.targets_tests import TargetTestBase


class TMissingOutputs(PythonTask):
    some_output = output
    forgotten_output = output

    def run(self):
        self.some_output.write("")


class TestTaskErrors(TargetTestBase):
    def test_no_outputs(self, capsys):
        with pytest.raises(DatabandExecutorError, match="Failed tasks are"):
            TMissingOutputs().dbnd_run()

    def test_to_read_input(self, capsys):
        class TCorruptedInput(PythonTask):
            some_input = data[DataFrame]
            forgotten_output = output

            def run(self):
                self.some_output.write("")

        t = self.target("some_input.json").write("corrupted dataframe")

        with pytest.raises(DatabandExecutorError, match="Failed tasks are"):
            TCorruptedInput(some_input=t).dbnd_run()
