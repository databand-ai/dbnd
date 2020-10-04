import logging

from dbnd import pipeline, task
from dbnd_test_scenarios.scheduler_scenarios import simplest_task


logger = logging.getLogger(__name__)


@task
def a():
    pass


@task
def b():
    pass


@task
def c():
    pass


@pipeline
def my_pipeline_inernal1(num):
    a()
    b()
    c()


@pipeline
def my_pipeline_inernal2(num):
    a()
    b()
    c()


@task
def my_task_2(num):
    for i in range(num):
        my_task_1(i)
    return my_pipeline_inernal2(num)


@task
def my_task_1(num):
    simplest_task()
    return my_pipeline_inernal1(num)


@pipeline
def large_subpipelines(width=10):
    result = my_task_1(width)
    for i in range(width):
        my_task_2(i)
    return result
