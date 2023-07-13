# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging
import random

from dbnd import dbnd_tracking
from dbnd._core.task_build.dbnd_decorator import task
from dbnd._core.tracking.metrics import log_metric


logger = logging.getLogger(__name__)

SIMPLE_SCENARIO_JOB_NAME = "dbnd_simple_tracking_scenario"
SIMPLE_SCENARIO_FUNC_NAME = "dbnd_tracking_func"
SIMPLE_SCENARIO_METRIC_INT_5 = "metric_check_int_5"


@task
def dbnd_tracking_func(check_time=datetime.datetime.now(), fail_chance=0.0):
    # type: (datetime.datetime, float) -> str
    logger.info("Running Databand Task Sanity Check!")

    log_metric("metric_check", "OK")
    log_metric(SIMPLE_SCENARIO_METRIC_INT_5, 5)

    if random.random() < fail_chance:
        raise Exception("Failed accordingly to fail_chance: %f" % fail_chance)

    log_metric("Happiness Level", "High")

    logger.info("Your system is good to go! Enjoy Databand!")
    return "Databand has been checked at %s" % check_time


def dbnd_simple_tracking_scenario(job_name=SIMPLE_SCENARIO_JOB_NAME):
    with dbnd_tracking(job_name=job_name):
        dbnd_tracking_func()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    dbnd_simple_tracking_scenario()
