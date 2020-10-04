from dbnd import data, output, parameter
from dbnd.tasks import PythonTask


class SomeTask(PythonTask):
    salad = output

    def run(self):
        self.log_metric("dressed", 1)

        self.salad.write("")


SomeTask().dbnd_run()
