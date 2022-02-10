import datetime
import logging
import random
import time

from dbnd._core.task_build.dbnd_decorator import task
from dbnd._core.tracking.metrics import log_metric


logger = logging.getLogger(__name__)


@task
def dbnd_sanity_check(check_time=datetime.datetime.now(), fail_chance=0.0):
    # type: (datetime.datetime, float) -> str
    logger.info("Running Databand Task Sanity Check!")

    log_metric("metric_check", "OK")

    if random.random() < fail_chance:
        raise Exception("Failed accordingly to fail_chance: %f" % fail_chance)

    log_metric("Happiness Level", "High")

    logger.info("Your system is good to go! Enjoy Databand!")
    return "Databand has been checked at %s" % check_time


@task
def dbnd_simple_task(
    check_time=datetime.datetime.now(), fail_chance=0.0, sleep=0.0, loop=1
):  # type: (datetime.datetime, float, float, int) -> str
    logger.info("Running dbnd_simple_task!")
    log_metric("metric_check", "OK")
    log_metric("metric_random_value", random.random())

    for current in range(loop):
        if current:
            logger.info("Loop %s out of %s", current, loop)
        if sleep:
            logger.info("Sleeping for %s seconds", sleep)
            time.sleep(sleep)

        if random.random() < fail_chance:
            raise Exception("Failed accordingly to fail_chance: %f" % fail_chance)

    return "Databand has been checked at %s" % check_time
