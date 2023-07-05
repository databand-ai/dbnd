# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd import data
from dbnd_run.tasks import PythonTask


logger = logging.getLogger(__name__)


class SimplestTask(PythonTask):
    some_input = data

    def run(self):
        logger.info("We are running some simplest code!")
        v = self.some_input.read()
        self.outputs.write(v)
