# © Copyright Databand.ai, an IBM Company 2022

from dbnd_examples.data import dbnd_examples_data_path
from dbnd_gcp.apache_beam.apache_beam_task import ApacheBeamPythonTask
from dbnd_gcp.apache_beam.parameters import beam_input, beam_output


class BeamWordCount(ApacheBeamPythonTask):
    py_file = dbnd_examples_data_path("dbnd_gcp/tool_dataflow/pybeam/wordcount.py")

    input = beam_input
    output = beam_output

    def get_beam_task_options(self):
        self.output.mkdir()
        return {"input": self.input, "output": self.output}
