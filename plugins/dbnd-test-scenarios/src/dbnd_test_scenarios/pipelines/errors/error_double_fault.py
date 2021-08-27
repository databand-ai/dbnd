import logging
import sys

from dbnd import PipelineTask, PythonTask, output, parameter


logger = logging.getLogger(__name__)


class T1(PythonTask):
    p1 = parameter.value("somep")
    o_1 = output[str]

    def run(self):
        self.o_1 = self.p1


class T2(PythonTask):
    p1 = parameter.value("somep")
    o_1 = output[str]

    def run(self):
        raise Exception()
        # self.o_1 = self.p1


class TPipe(PipelineTask):
    o_1 = output[str]
    o_2 = output[str]

    def band(self):
        self.o_1 = T1().o_1
        self.o_2 = T2(p1=self.o_1)


if __name__ == "__main__":
    TPipe(override={T1.task_version: "now"}).dbnd_run()
