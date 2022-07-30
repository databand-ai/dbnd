# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging

from random import shuffle
from typing import List

from dbnd import log_metric, output, pipeline, task
from dbnd.errors import TaskValidationError
from dbnd_examples.data import data_repo
from targets import Target
from targets.types import DataList


#
# language=bash
TUTORIAL = """
# Execute without databand
python dbnd_examplesExecute full pipeline
dbnd run prepare_salad

# Execute task
dbnd run cut --vegetables data/vegetables_for_pasta_salad.txt
"""


@task
def add_dressing(chopped_vegetables, dressing, salt_amount="low"):
    # type: (DataList[str], str, str) -> List[str]
    # adds dressing
    if salt_amount == "very_high":
        raise TaskValidationError("Salt level is too high!")

    mix = [
        "%s->%s  (%s salt)" % (dressing, v.rstrip(), salt_amount)
        for v in chopped_vegetables
    ]

    log_metric("dressed", len(mix))
    log_metric("dressing", dressing)
    log_metric("dict", {"a": datetime.datetime.utcnow(), "tuple": ("a", 1)})

    logging.info("Dressing result %s", mix)
    return [x + "\n" for x in mix]


@task(result=output().data_list_str)
def cut(vegetables):
    # type: (DataList[str]) -> DataList[str]
    # cuts vegetables
    chopped = []

    logging.info(
        "Got {}. Start Chopping.".format(",".join(vegetables)).replace("\n", "")
    )

    for line in vegetables:
        chopped.extend(list(line.rstrip()))

    shuffle(chopped)

    logging.info("Chopped vegetables:" + " ".join(chopped))

    return [x + "\n" for x in chopped]


@pipeline
def prepare_salad(vegetables=data_repo.vegetables, dressing="oil"):
    # type: (Target, str) -> List[str]
    return add_dressing(cut(vegetables), dressing)


if __name__ == "__main__":
    with open(data_repo.vegetables, "r") as f:
        content = f.readlines()

    salad = prepare_salad(content, "mayo")

    with open("output.txt", "w") as f:
        f.writelines(salad)
