# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd._core.parameter.parameter_builder import output, parameter
from dbnd_run.tasks import PipelineTask, PythonTask


logger = logging.getLogger(__name__)


class SimplestTask(PythonTask):
    simplest_param = parameter.value("1")
    simplest_output = output

    def run(self):
        logger.info("We are running some simplest code!")
        self.simplest_output.write(self.simplest_param)


class SimplestPipeline(PipelineTask):
    simplest_output = output

    def band(self):
        self.simplest_output = SimplestTask().simplest_output


class SimpleTask(PythonTask):
    simple_input = parameter.data
    simple_output = output

    def run(self):
        v = self.simple_input.read()

        logger.info("We are running some simple code: got string of length %s", len(v))
        self.simple_output.write(v)
