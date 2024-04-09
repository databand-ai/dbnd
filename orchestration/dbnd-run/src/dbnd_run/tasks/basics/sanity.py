# © Copyright Databand.ai, an IBM Company 2022

import datetime
import logging
import random
import time

from dbnd._core.task_build.dbnd_decorator import pipeline, task
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


@pipeline
def dbnd_simple_parallel_pipeline(tasks_num=2):
    return {
        str(i): dbnd_simple_task(task_name=f"dbnd_simple_task_{i}")
        for i in range(tasks_num)
    }


@task
def dbnd_sub_run():
    logger.info("STARTING THE RUN")
    from dbnd import get_databand_context

    databand_context = get_databand_context()
    databand_context.run_settings.run.submit_driver = False
    databand_context.run_settings.run.is_archived = True
    from dbnd._core.utils.timezone import utcnow

    databand_context.run_settings.run.execution_date = utcnow()

    result = dbnd_simple_task.dbnd_run(
        task_name="dbnd_sub_simple_run", task_version=str(utcnow())
    )

    logger.info("FINISHED THE RUN")

    return result.root_task.result.load(str)


@pipeline
def dbnd_pipeline_with_sub(tasks_num=2):
    tasks = {}
    # tasks = {
    #     str(i): dbnd_simple_task(task_name=f"dbnd_simple_task_{i}")
    #     for i in range(tasks_num)
    # }
    tasks["sub_run"] = dbnd_sub_run()
    return tasks
