import logging
import os
import shutil

from contextlib import contextmanager
from tempfile import mkdtemp, mkstemp
from typing import Type

from dbnd._core.parameter.parameter_definition import (
    ParameterDefinition,
    _ParameterKind,
)
from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._vendor._marshmallow.compat import urlparse
from targets import (
    DbndLocalFileMetadataRegistry,
    DirTarget,
    FileAlreadyExists,
    FileTarget,
    LocalFileSystem,
    target,
)
from targets.caching import get_or_create_folder_in_dir
from targets.multi_target import MultiTarget


logger = logging.getLogger(__name__)

LOCAL_SYNC_CACHE_NAME = "local_sync_cache"


class TaskRunLocalSyncer(TaskRunCtrl):
    """
    This ctrl is in charge of syncing all required files/folders locally for execution (direct read/write)
    """

    def __init__(self, task_run):
        super(TaskRunLocalSyncer, self).__init__(task_run=task_run)
        self.inputs_to_sync = []
        self.outputs_to_sync = []
        self.local_sync_root = self.task_env.dbnd_local_root.partition("local_sync")

        for p_def, p_val in self.task._params.get_param_values(user_only=True):
            if (
                isinstance(p_val, FileTarget)
                and p_val.config.require_local_access
                and not p_val.fs.local
            ):

                self._sync_input_or_output(p_def, p_val)

            elif isinstance(p_val, MultiTarget):
                self._sync_input_or_output(p_def, p_val)

    def _local_cache_target(self, target_):
        # type: (FileTarget) -> FileTarget
        # urlparse is needed to strip filesystem prefixes like `s3://`
        path = urlparse.urlparse(target_.path)
        path = path.netloc + path.path
        path = path.lstrip("/")
        local_sync_cache_path = get_or_create_folder_in_dir(
            LOCAL_SYNC_CACHE_NAME, self.task.task_env.dbnd_local_root.path
        )
        return target(local_sync_cache_path, path, config=target_.config,)

    def _local_cache_multitarget(self, multitarget):
        return MultiTarget(
            [
                self._local_cache_target(target_)
                if target_.config.require_local_access and not target_.fs.local
                else target_
                for target_ in multitarget.targets
            ]
        )

    def _sync_input_or_output(self, param_definition, old_target):
        # type: (ParameterDefinition, Type[DataTarget], Type[DataTarget]) -> None
        if param_definition.kind == _ParameterKind.task_output:
            # Output should be substituted for local path and synced post execution
            self.outputs_to_sync.append((param_definition, old_target))

        else:
            self.inputs_to_sync.append((param_definition, old_target))

    def sync_pre_execute(self):
        for p_def, remote_target in self.inputs_to_sync:
            # Input should be synced to local path and substituted
            try:
                if isinstance(remote_target, MultiTarget):
                    local_target = self._local_cache_multitarget(remote_target)

                    logger.info(
                        "Downloading  %s %s to %s", p_def, remote_target, local_target,
                    )

                    for remote_subtarget, local_subtarget in zip(
                        remote_target.targets, local_target.targets
                    ):
                        if (
                            remote_subtarget.config.require_local_access
                            and not remote_subtarget.fs.local
                        ):
                            self._sync_remote_to_local(
                                remote_subtarget, local_subtarget
                            )
                else:
                    # Target requires local access, it points to a remote path that must be synced-to from a local path
                    local_target = self._local_cache_target(remote_target)

                    logger.info(
                        "Downloading  %s %s to %s", p_def, remote_target, local_target,
                    )

                    self._sync_remote_to_local(remote_target, local_target)

            except Exception as e:
                logger.exception(
                    "Failed to create local cache for %s %s", p_def, remote_target,
                )
                raise

            setattr(self.task, p_def.name, local_target)

        logger.info(
            "All required task inputs are downloaded to %s",
            self.task_run.attemp_folder_local_cache,
        )

        for p_def, remote_target in self.outputs_to_sync:
            if isinstance(remote_target, MultiTarget):
                local_target = self._local_cache_multitarget(remote_target)
                for remote_subtarget, local_subtarget in zip(
                    remote_target.targets, local_target.targets
                ):
                    if (
                        remote_subtarget.config.require_local_access
                        and not remote_subtarget.fs.local
                    ):
                        local_subtarget.mkdir_parent()
            else:
                local_target = self._local_cache_target(remote_target)
                local_target.mkdir_parent()
            # Output should be substituted for local path and synced post execution
            setattr(self.task, p_def.name, local_target)

    def sync_post_execute(self):
        for p_def, remote_target in self.inputs_to_sync:
            setattr(self.task, p_def.name, remote_target)

        for p_def, remote_target in self.outputs_to_sync:
            try:
                if isinstance(remote_target, MultiTarget):
                    local_target = self._local_cache_multitarget(remote_target)

                    logger.info(
                        "Uploading  %s %s from %s", p_def, remote_target, local_target
                    )

                    for remote_subtarget, local_subtarget in zip(
                        remote_target.targets, local_target.targets
                    ):
                        if (
                            remote_subtarget.config.require_local_access
                            and not remote_subtarget.fs.local
                        ):
                            remote_subtarget.copy_from_local(
                                local_path=local_subtarget.path
                            )
                            if remote_subtarget.config.flag:
                                remote_subtarget.mark_success()

                else:
                    local_target = self._local_cache_target(remote_target)

                    logger.info(
                        "Uploading  %s %s from %s", p_def, remote_target, local_target
                    )

                    remote_target.copy_from_local(local_path=local_target.path)
                    if remote_target.config.flag:
                        remote_target.mark_success()
            except Exception:
                logger.exception(
                    "Failed to upload task output %s %s from %s",
                    p_def,
                    remote_target,
                    local_target,
                )
                raise
            setattr(self.task, p_def.name, remote_target)

    def _sync_remote_to_local(self, remote_target, local_target):
        # type: (FileTarget, FileTarget) -> None
        # Use DbndLocalFileMetadataRegistry to sync inputs only when necessary
        dbnd_meta_cache = DbndLocalFileMetadataRegistry.get_or_create(local_target)

        # When syncing remote to local -> We can't use MD5, so use TTL instead
        if dbnd_meta_cache.expired or not local_target.exists():
            # If TTL is invalid, or local file doesn't exist -> Sync to local
            local_target.mkdir_parent()

            with local_target.tmp() as tmp_local_path:
                remote_target.download(tmp_local_path)

        DbndLocalFileMetadataRegistry.refresh(local_target)
