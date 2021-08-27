import time

from typing import List

from dbnd import parameter, pipeline, task
from dbnd._core.current import current_task
from dbnd._core.utils.basics.nothing import NOTHING
from targets import Target


@task
def slow_task(inputs=parameter(empty_default=True)[List[Target]], sleep_sec=5):
    time.sleep(sleep_sec)
    return current_task().task_id


@pipeline
def tasks():
    first = slow_task(task_name="first")
    p_tasks = [slow_task(task_name=" p_%s" % i, inputs=[first]) for i in range(4)]
    last = slow_task(task_name="last", inputs=p_tasks)
    return last
