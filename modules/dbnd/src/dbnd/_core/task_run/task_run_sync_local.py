import logging
import os

from dbnd._core.parameter.parameter_definition import _ParameterKind
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from targets import DirTarget, FileTarget, target


logger = logging.getLogger(__name__)


class TaskRunLocalSyncer(TaskRunCtrl):
    """
    This ctrl is in charge of syncing all required files/folders locally for execution (direct read/write)
    """

    def __init__(self, task_run):
        super(TaskRunLocalSyncer, self).__init__(task_run=task_run)
        self.inputs_to_sync = []
        self.outputs_to_sync = []

        for p_def, p_val in self.task._params.get_param_values(user_only=True):
            if (
                isinstance(p_val, FileTarget)
                and p_val.config.require_local_access
                and not p_val.fs.local
            ):
                # Target requires local access, it points to a remote path that must be synced-to from a local path
                local_target = target(
                    self.task_run.attemp_folder_local_cache,
                    os.path.basename(p_val.path),
                )
                if p_def.kind == _ParameterKind.task_output:
                    # Output should be substituted for local path and synced post execution
                    self.outputs_to_sync.append((p_def, p_val, local_target))
                else:
                    # Input should be synced to local path and substituted
                    self.inputs_to_sync.append((p_def, p_val, local_target))

    def sync_pre_execute(self):
        if self.inputs_to_sync:
            for p_def, p_val, local_target in self.inputs_to_sync:
                # Input should be synced to local path and substituted
                try:
                    logger.info("Downloading  %s %s to %s", p_def, p_val, local_target)
                    local_target.mkdir_parent()
                    p_val.download(local_target.path)
                except Exception as ex:
                    logger.exception(
                        "Failed to create local cache for %s %s at %s",
                        p_def,
                        p_val,
                        local_target,
                    )
                    raise
                setattr(self.task, p_def.name, local_target)
            logger.info(
                "All required task inputs are downloaded to %s",
                self.task_run.attemp_folder_local_cache,
            )

        for p_def, p_val, local_target in self.outputs_to_sync:
            local_target.mkdir_parent()
            # Output should be substituted for local path and synced post execution
            setattr(self.task, p_def.name, local_target)

    def sync_post_execute(self):
        for p_def, p_val, local_target in self.inputs_to_sync:
            setattr(self.task, p_def.name, p_val)

        for p_def, p_val, local_target in self.outputs_to_sync:
            try:
                logger.info("Uploading  %s %s from %s", p_def, p_val, local_target)
                p_val.copy_from_local(local_path=local_target.path)
                if p_val.config.flag:
                    p_val.mark_success()
            except Exception as ex:
                logger.exception(
                    "Failed to upload task output %s %s from %s",
                    p_def,
                    p_val,
                    local_target,
                )
                raise
            setattr(self.task, p_def.name, p_val)
