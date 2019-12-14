from time import sleep

from dbnd import pipeline, task
from dbnd._core.commands import log_metric
from dbnd._core.current import current_task_run


@task
def operation_int(
    input_a, input_b=0, pause=0.0, log_metrics=True, external_resources=0
):
    # type: (int, int, float, bool, int) -> int
    if log_metrics:
        log_metric("input_a", input_a)
        log_metric("input_b", input_b)

    tr = current_task_run()
    for i in range(external_resources):
        tr.set_external_resource_urls(
            {"url_%s_%d" % (tr.task.task_id, i): "http://localhost"}
        )
    if pause:
        sleep(pause)
    return input_a + input_b


@task
def init_acc_int(input=0):
    # type: (int) -> int
    log_metric("input", input)
    return input


@pipeline
def large_pipe_int(width=10, depth=1, pause=0.0):
    # type: (int, int, float) -> int
    res = init_acc_int()
    for i in range(width):
        acc = init_acc_int()
        for d in range(depth):
            acc = operation_int(acc, i, pause)
        res = operation_int(res, acc, pause)

    return res
