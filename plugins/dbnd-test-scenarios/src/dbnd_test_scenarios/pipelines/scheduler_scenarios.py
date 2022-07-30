# © Copyright Databand.ai, an IBM Company 2022

from random import random
from time import sleep

from dbnd import pipeline, task


@task
def fail_randomly(fail_chance=0.5):
    """made to test retry scenarios in the scheduler"""

    if random() < fail_chance:
        raise Exception("This day I fail")
    else:
        return "But on this day I succeed!"


@task
def simplest_task():
    return "this is the simplest possible task"


@task
def long_task(duration_s=600):
    """made to test scheduler behaviour with parallel job execution"""

    print("sleeping for %s seconds" % duration_s)
    sleep(duration_s)
    return "good morning"


@pipeline
def long_pipeline():
    simplest_task()
    long_task()
