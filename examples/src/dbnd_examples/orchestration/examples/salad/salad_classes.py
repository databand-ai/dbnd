import logging

from random import shuffle
from typing import List

from dbnd import data, output, parameter
from dbnd.errors import TaskValidationError
from dbnd.tasks import PipelineTask, PythonTask
from dbnd_examples.data import data_repo
from targets.types import DataList


TUTORIAL = """
# Execute full pipeline
dbnd run PrepareSalad

# now we don't run anything
dbnd run PrepareSalad

# now we run our pipeline with some production
dbnd run PrepareSalad --vegetables data/vegetables_for_pasta_salad.txt

# now we run our pipeline with some salt
dbnd run PrepareSalad  --AddDressing-salt-amount very_high

# now we run our pipeline with some sals with not errors
dbnd run PrepareSalad  --AddDressing-salt-amount high

# now we run same flow in production
dbnd run PrepareSalad --env prod

dbnd run LunchWithSalad  --env prod --engine gc

#  experiment flow history
dbnd run PrepareSalad  --name exp_started
dbnd run PrepareSalad --AddDressing-versiond2 --name exp_v2
dbnd run PrepareSalad --task-version d3 --name exp_bad_dressing
dbnd run PrepareSalad --run-task AddDressing --task-version d3 --name exp_better_dressing
dbnd run PrepareSalad --task-version d3 --name exp_finalize_dressing

dbnd run LunchWithSalad --task-version d3 --name exp_finalize_dressing
dbnd run LunchWithSalad --task-version d3 --name exp_finalize_dressing --AddDressing-salt-amount high

"""


class Cut(PythonTask):
    vegetables = data[DataList[str]]
    chopped_vegetables = output[List[str]]

    def run(self):
        chopped = []

        vegg_message = ",".join(self.vegetables).replace("\n", "")
        logging.info("Got %s. Start Chopping.", vegg_message)

        for line in self.vegetables:
            chopped.extend(list(line.rstrip()))

        self.log_metric("chopped", len(chopped))

        shuffle(chopped)

        result = "".join(chopped)
        logging.info("Chopped vegetables: %s", result)

        self.chopped_vegetables = result


class AddDressing(PythonTask):
    dressing = parameter[str]
    salt_amount = parameter.value("low")
    chopped_vegetables = data[DataList[str]]
    salad = output

    def run(self):
        veg = self.chopped_vegetables

        if self.salt_amount == "very_high":
            raise TaskValidationError("Salt level is too high!")

        mix = ["%s->%s  (%s salt)" % (self.dressing, v, self.salt_amount) for v in veg]

        self.log_metric("dressed", len(mix))

        result = "".join(mix)
        self.salad.write(result)
        logging.info("Dressing result %s", result)


class PrepareSalad(PipelineTask):
    vegetables = data(default=data_repo.vegetables).target
    dressing = parameter.value(default="oil", description="dressing for the salad")

    salad = output

    def band(self):
        s1 = Cut(vegetables=self.vegetables)
        self.salad = AddDressing(
            chopped_vegetables=s1.chopped_vegetables, dressing=self.dressing
        ).salad


# PRODUCTION PART
class BuyVegetables(PythonTask):
    vegetables = output[List[str]]
    bill = output[str]

    def run(self):
        vegetables = ["tomato", "cucumber", "onions"]
        self.vegetables = "\n".join(vegetables)
        self.bill = "$5"


class EatSalad(PythonTask):
    salad = data[List[str]]
    feedback = output.data

    def run(self):
        for l in self.salad:
            logging.warning("EATING SALAD: %s", l)
        self.feedback.write("Salad was good!")


class LunchWithSalad(PipelineTask):
    vegs = output.data
    lunch = output.data

    def band(self):
        vegs = BuyVegetables()
        ready_salad = PrepareSalad(vegetables=vegs.vegetables, dressing="balsami")

        self.vegs = vegs
        self.lunch = EatSalad(salad=ready_salad.salad)
