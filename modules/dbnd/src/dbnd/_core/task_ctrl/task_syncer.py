import os

from dbnd._core.parameter.parameter_definition import _ParameterKind
from dbnd._core.parameter.parameter_value import ParameterValue
from dbnd._core.task_ctrl.task_ctrl import TaskSubCtrl
from targets import Target, target
from targets.values import TargetValueType


def generate_local_path(remote_path):
    from dbnd import current_task, get_databand_context

    dc = get_databand_context()
    local_cache_dir = dc.env.dbnd_local_root.folder("local_cache")
    curr_task = current_task()
    remote_basename = os.path.basename(remote_path)
    local_path = os.path.join(
        local_cache_dir, curr_task.task_signature, remote_basename
    )
    local_dirpath = os.path.dirname(local_path)
    os.makedirs(local_dirpath, exist_ok=True)
    return local_path


class TaskSyncer(TaskSubCtrl):
    def __init__(self, task):
        super(TaskSyncer, self).__init__(task)
        self.outputs_to_sync = []

    def sync_pre_execute(self):
        for p_def, p_val in self.task._params.get_param_values(user_only=True):
            if isinstance(p_val, Target) and p_val.config.require_local_access:
                # Target requires local access, it points to a remote path that must be synced-to from a local path
                local_path = generate_local_path(p_val.path)
                local_target = target(local_path)
                remote_target = p_val
                if p_def.kind == _ParameterKind.task_output:
                    # Output should be substituted for local path and synced post execution
                    setattr(self.task, p_def.name, local_target)
                    self.outputs_to_sync.append((local_target, remote_target))
                else:
                    # Input should be synced to local path and substituted
                    remote_target.fs.download(remote_target.path, local_target.path)
                    setattr(self.task, p_def.name, local_target)

    def sync_post_execute(self):
        for local_target, remote_target in self.outputs_to_sync:
            remote_target.copy_from_local(local_path=local_target.path)
