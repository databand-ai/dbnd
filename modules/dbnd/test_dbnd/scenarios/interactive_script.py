# © Copyright Databand.ai, an IBM Company 2022

from dbnd import output
from dbnd_run.tasks import PythonTask


class SomeTask(PythonTask):
    salad = output

    def run(self):
        self.log_metric("dressed", 1)

        self.salad.write("")


SomeTask().dbnd_run()
