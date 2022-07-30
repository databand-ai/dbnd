# © Copyright Databand.ai, an IBM Company 2022

"""To run this example run the following command:
dbnd  run dbnd_test_scenarios.pipelines.errors.canceled_run_pipeline.pipeline_that_cancel_itself  --task-version=now  --set long_sleeping_task.sleep_sec=20 --parallel
"""
import logging
import time

from dbnd import current_task_run, get_databand_run, pipeline, task


logger = logging.getLogger(__name__)


@task
def long_sleeping_task(sleep_sec=10):
    logger.info("I am going to sleep, wake me with cancel, please!")
    time.sleep(sleep_sec)
    current_task_run()
    return True


@task
def cancel_run():
    print("Going to kill current run")
    databand_run = get_databand_run()
    r = databand_run.kill_run()
    print("Killed run")
    import time

    # Wait for task run to die
    time.sleep(5)
    return r


@task
def task_that_never_run(input):
    return input


@pipeline
def pipeline_that_cancel_itself():
    cancel_run_task = cancel_run()
    sleep_task = long_sleeping_task()
    t = task_that_never_run(sleep_task)
    return cancel_run_task, t
