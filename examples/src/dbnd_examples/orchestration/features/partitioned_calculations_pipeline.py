# © Copyright Databand.ai, an IBM Company 2022

import logging

import pandas as pd

from dbnd import PipelineTask, PythonTask, output, parameter


logger = logging.getLogger(__name__)


class PStepA(PythonTask):
    """First step of partitioned calculations"""

    data_partitioned = output.folder.csv.data

    # noinspection PyMethodMayBeStatic
    def _yield_data(self):
        for i in range(10):
            data = [["some_value", i]]
            yield pd.DataFrame(data=data, columns=["Names", "Value"])

    def run(self):
        self.data_partitioned.write_df(self._yield_data())


# noinspection PyMethodMayBeStatic
class PStepB(PythonTask):
    """Second step of partitioned calculations"""

    data_partitioned = parameter.data
    step_b_value = output.folder.csv.data

    def _stream_function(self, data_gen):
        for partition in data_gen:
            partition["StepB"] = partition["Value"] * 3
            yield partition

    def run(self):
        data = self.data_partitioned.read_df_partitioned()
        self.step_b_value.write_df(self._stream_function(data))


class ExamplePartitionedCalculations(PipelineTask):
    """Entry point of partitioned calculations"""

    step_b_value = output.data

    def band(self):
        data_partitioned = PStepA().data_partitioned
        self.step_b_value = PStepB(data_partitioned=data_partitioned).step_b_value
