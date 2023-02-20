# © Copyright Databand.ai, an IBM Company 2022

from time import sleep

from dbnd import log_metric, pipeline, task
from dbnd._core.current import current_task_run


@task
def operation_int(
    input_a: int,
    input_b: int = 0,
    pause: int = 0.0,
    log_metrics: bool = True,
    external_resources: int = 0,
) -> int:
    if log_metrics:
        log_metric("input_a", input_a)
        log_metric("input_b", input_b)

    tr = current_task_run()
    for i in range(external_resources):
        tr.set_external_resource_urls(
            {f"url_{tr.task.task_id}_{i:d}": "http://localhost"}
        )
    if pause:
        sleep(pause)
    return input_a + input_b


@task
def init_acc_int(input_value: int = 0) -> int:
    log_metric("input", input_value)
    return input_value


@pipeline
def large_pipe_int(width: int = 10, depth: int = 1, pause: float = 0.0) -> int:
    res = init_acc_int()
    for i in range(width):
        acc = init_acc_int()
        for d in range(depth):
            acc = operation_int(acc, i, pause)
        res = operation_int(res, acc, pause)

    return res
