import logging
import time

from datetime import timedelta
from typing import List

from dbnd import pipeline, task


logger = logging.getLogger(__name__)


@task
def long_time_running_task(
    p_input="input", p_add="add", sleep_time=timedelta(seconds=60)
):
    if sleep_time:
        logger.warning("Sleeping for %s ", sleep_time)
        time.sleep(sleep_time.total_seconds())
    return "%s_%s" % (p_input, p_add)


@task
def combine_all_inputs(p_input_list):
    # type: (List[str]) -> str

    return " ".join(p_input_list)


@pipeline
def pipe_of_long_tasks(num_per_iteration=3, iterations=1):
    p_input = ""
    for iter_id in range(iterations):
        iter_task_results = []
        for t_id in range(num_per_iteration):
            iter_task_results.append(
                long_time_running_task(p_input, p_add="%s_%s" % (iter_id, t_id))
            )
        p_input = combine_all_inputs(iter_task_results)
    return p_input


def user_exception_task(p_input=""):
    raise Exception("An explicit error for crushing this pipeline.")
