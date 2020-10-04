from dbnd._core.utils.task_utils import to_targets
from dbnd._core.utils.traversing import flatten
from targets.multi_target import MultiTarget


def data_combine(inputs, sort=False):
    targets = flatten(to_targets(inputs))
    if sort:
        targets = sorted(targets, key=lambda x: x.path)
    data = MultiTarget(targets)
    return data
