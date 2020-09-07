from time import sleep

from dbnd import pipeline, task


@task
def task_innermost(n):
    sleep(1)


@task
def task_inner(n):
    sleep(1)
    task_innermost(n)


@task
def task(n):
    sleep(1)
    task_inner(n)


@pipeline
def tasks_within_tasks():
    task(1)
    task(2)
