import logging
import os

from dbnd_luigi.luigi_params import get_dbnd_param_by_luigi_name
from targets import target


logger = logging.getLogger(__name__)


def extract_targets(luigi_task):
    yield from iterate_targets(luigi_task.output(), luigi_task.task_id, is_output=True)
    yield from iterate_targets(luigi_task.input(), luigi_task.task_id, is_output=False)


def iterate_targets(luigi_target, task_id, is_output, name=None):
    if luigi_target is None:
        return

    elif isinstance(luigi_target, dict):
        # Multiple targets, dict object
        for name, val in luigi_target.items():
            yield from iterate_targets(val, task_id, is_output, name)

    elif isinstance(luigi_target, list):
        for t in luigi_target:
            yield from iterate_targets(t, task_id, is_output)

    else:
        # Single target, target object
        dbnd_target_param = _convert_to_dbnd_target_param(
            luigi_target, task_id, is_output=is_output
        )
        if dbnd_target_param:
            parameter_name = name or os.path.basename(luigi_target.path)
            yield parameter_name, dbnd_target_param


def _convert_to_dbnd_target_param(luigi_target, task_id, is_output):
    from luigi.target import FileSystemTarget

    # TODO: Run with s3 target
    if isinstance(luigi_target, FileSystemTarget):
        return _build_filesystem_target(luigi_target, task_id, is_output)


def _build_filesystem_target(luigi_target, task_id, is_output):
    from dbnd._core.task_ctrl.task_relations import traverse_and_set_target
    from targets.base_target import TargetSource

    dbnd_target = target(luigi_target.path)
    fixed_target = traverse_and_set_target(dbnd_target, TargetSource(task_id=task_id))
    target_param = (
        get_dbnd_param_by_luigi_name("TargetParameter")
        .target_config(fixed_target.config)
        .default(fixed_target.path)()
    )
    if is_output:
        target_param = target_param.output()
    return target_param
