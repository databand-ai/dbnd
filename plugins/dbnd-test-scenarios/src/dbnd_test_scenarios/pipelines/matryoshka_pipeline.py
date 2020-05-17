from dbnd._core.decorator.func_task_decorator import pipeline


@pipeline
def matryoshka():
    a()


@pipeline
def a():
    b()


@pipeline
def b():
    c()


@pipeline
def c():
    return
