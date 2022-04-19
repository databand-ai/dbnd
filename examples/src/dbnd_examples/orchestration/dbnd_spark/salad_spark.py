"""
# Run specific task, increment task version
dbnd run dbnd_examples.orchestration.dbnd_spark.salad_spark.CutAtSpark --set  vegetables=README.md
dbnd run dbnd_examples.orchestration.dbnd_spark.salad_spark.PrepareSaladAtSpark
"""

import logging

from dbnd import data, output, parameter
from dbnd.tasks import PipelineTask
from dbnd_examples.data import data_repo
from dbnd_examples.orchestration.dbnd_spark.scripts import spark_script
from dbnd_spark import PySparkTask


logger = logging.getLogger(__name__)


class CutAtSpark(PySparkTask):
    python_script = spark_script("cut_salad.py")

    vegetables = parameter.data
    chopped_vegetables = output

    def application_args(self):
        return [self.vegetables, self.chopped_vegetables]


class AddDressingAtSpark(PySparkTask):
    python_script = spark_script("add_dressing.py")

    dressing = parameter[str]
    chopped_vegetables = parameter.data
    salad = output.data

    def application_args(self):
        return [self.chopped_vegetables, self.dressing, self.salad]


class PrepareSaladAtSpark(PipelineTask):
    vegetables = data(default=data_repo.vegetables)
    dressing = parameter.value("oil")

    salad = output.data

    def band(self):
        s1 = CutAtSpark(vegetables=self.vegetables)
        self.salad = AddDressingAtSpark(
            chopped_vegetables=s1.chopped_vegetables, dressing=self.dressing
        )
