# © Copyright Databand.ai, an IBM Company 2022
import logging
import random

from dbnd import dbnd_tracking, log_metric


def dbnd_sanity_tracked_func():
    log_metric("from_dbnd_sanity_tracked_func_5", 5)
    log_metric("from_dbnd_sanity_tracked_func_rand", random.randint(0, 100))


def dbnd_sanity_check_tracking():
    with dbnd_tracking(job_name="dbnd_sanity_check_tracking"):
        dbnd_sanity_tracked_func()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    dbnd_sanity_check_tracking()
