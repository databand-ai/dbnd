import logging
import time

from typing import List

from dbnd import parameter, pipeline, task
from dbnd._core.current import current_task
from targets import Target


logger = logging.getLogger(__name__)


@task
def slow_task(inputs=parameter(empty_default=True)[List[Target]], sleep_sec=5):
    for i in range(sleep_sec):
        logger.info("Sleeping for 1 sec (%s from %s)", i, sleep_sec)
        time.sleep(1)
    return current_task().task_id


@pipeline
def pipeline_of_slow_tasks():
    first = slow_task(task_name="first")
    p_tasks = [slow_task(task_name=" p_%s" % i, inputs=[first]) for i in range(4)]
    last = slow_task(task_name="last", inputs=p_tasks)
    return last
