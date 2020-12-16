from dbnd import task


@task
def subtask(s):
    return s + s


@task
def task_with_subtasks():
    res1 = subtask("la ")
    res2 = subtask(res1)
    return subtask(res2)
