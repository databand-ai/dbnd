import datetime
import logging
import random

from dbnd._core.task_build.dbnd_decorator import task
from dbnd._core.tracking.metrics import log_metric


logger = logging.getLogger(__name__)


@task
def dbnd_sanity_check(check_time=datetime.datetime.now()):
    # type: ( datetime.datetime)-> str
    logger.info("Running Sanity Check!")
    log_metric("Happiness Level", "High")
    logger.info("Your system is good to go! Enjoy Databand!")
    return "Databand checked at %s" % check_time


@task
def dbnd_random_check(check_time=datetime.datetime.now(), fail_chance=0.5):
    assert random.random() > fail_chance
    return "Databand checked at %s" % check_time
