# =======================
import logging

from dbnd import PipelineTask, PythonTask, output, parameter
from test_dbnd.scenarios import data


logger = logging.getLogger(__name__)


class SF_A(PythonTask):
    my_filter = parameter[int]

    input_logs = data
    input_labels = data

    o_devices = output[str]
    o_stats = output.target

    def run(self):
        self.o_devices = "devices.."
        self.o_stats.write("stats\t1")


class SF_B(PythonTask):
    combine_similar = parameter[bool]

    input_devices = data

    o_device_histogram = output
    o_types = output

    def run(self):
        self.o_device_histogram.write("1\t2")
        self.o_types.write("mobile\t1")


class SF_C(PythonTask):
    input_devices = data
    input_types = data

    o_report = output
    o_model = output

    def run(self):
        self.o_report.write("good\t2")
        self.o_model.write("model xml")


class ExampleSF(PipelineTask):
    input_data = data
    combine_similar = parameter[bool]

    some_output = output

    def band(self):
        s1 = SF_A(input_logs=self.input_data, input_labels=self.input_data)
        s2 = SF_B(input_devices=s1.o_devices, combine_similar=self.combine_similar)
        self.some_output = SF_C(input_devices=s1.o_devices, input_types=s2.o_types)


class SF_E(PythonTask):
    data = data
    output = output

    def run(self):
        self.output.write("")
