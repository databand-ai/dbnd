from dbnd import pipeline, task


@task
def task_A() -> int:
    x = 3 / 0
    return 1


@task
def task_B() -> int:
    return 2


@task
def task_A1(num: int) -> int:
    return num + 1


@task
def task_B1(num: int) -> int:
    return num + 2


@task
def task_A2(x: int):
    print(x)


@task
def task_B2(y: int):
    print(y)


@pipeline
def test_pipeline():
    a = task_A()
    a1 = task_A1(a)
    task_A2(a1)
    b = task_B()
    b1 = task_B1(b)
    task_B2(b1)
