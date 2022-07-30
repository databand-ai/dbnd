# © Copyright Databand.ai, an IBM Company 2022

from time import sleep

from dbnd import pipeline, task


@task
def subtask(s):
    return s + s


@task
def task_with_subtasks():
    res1 = subtask("la ")
    res2 = subtask(res1)
    return subtask(res2)


@task
def task_innermost(n):
    sleep(1)


@task
def task_inner(n):
    sleep(1)
    task_innermost(n)


@task
def task1(n):
    sleep(1)
    task_inner(n)


@pipeline
def tasks_within_tasks():
    task1(1)
    task1(2)
