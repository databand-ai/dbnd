# © Copyright Databand.ai, an IBM Company 2022

import hashlib
import logging

from os import path

import six

from dbnd._core.utils.task_utils import targets_to_str
from dbnd_run.task_ctrl import _TaskRunExecutorCtrl
from targets import Target, target
from targets.fs import FileSystems


logger = logging.getLogger(__name__)


class TaskSyncCtrl(_TaskRunExecutorCtrl):
    def __init__(self, task_run):
        super(TaskSyncCtrl, self).__init__(task_run=task_run)

        self.remote_sync_root = self.task.task_env.dbnd_data_sync_root.folder("deploy")

    def sync_files(self, local_files):
        if not local_files:
            return []
        return [self.sync(f) for f in local_files]

    def sync(self, local_file):
        if not local_file:
            #  should return None, not empty string to be compatible with airflow code
            return None

        if not isinstance(local_file, Target):
            local_file = target(local_file)
        return str(self._sync(local_file))

    def arg_files(self, local_files):
        #  should return None, not empty string to be compatible with airflow code
        if not local_files:
            return None
        if isinstance(local_files, six.string_types):
            local_files = [local_files]
        synced_files = self.sync_files(local_files)
        synced_files_str = targets_to_str(synced_files)
        return ",".join(synced_files_str)

    def is_remote(self, file):
        if not file:
            return None
        return file.fs.name != FileSystems.local

    def _md5(self, local_path):
        with open(local_path, "rb+") as f:
            checksum = hashlib.md5(f.read())  # nosec B324
        return checksum.hexdigest()

    def remote_file(self, local_file, md5_hash):
        if self.is_remote(local_file):
            return local_file

        file_name = path.basename(local_file.path)
        remote_file_name = "{}/{}".format(md5_hash, file_name)
        return self.remote_sync_root.partition(remote_file_name)

    def _sync_remote(self, remote_file):
        """
        Synchronizes remote fs -> local fs
        """
        return remote_file

    def _sync_local(self, local_file):
        """
        Synchronizes local fs -> remote fs
        """
        md5_hash = self._md5(local_file.path)
        remote_file = self.remote_file(local_file, md5_hash)

        if not remote_file.exists():
            try:
                self._upload(local_file, remote_file)
            except:
                if not remote_file.exists():
                    logger.error(
                        "Unexpected error occurred while trying to upload file `%s` to the remote server",
                        remote_file,
                    )
                    raise
                logger.warning(
                    "File %s was copied from different process to the remote location, skipping upload...",
                    remote_file,
                )

        else:
            logger.info(
                "File %s already exists at remote location, skipping upload...",
                remote_file,
            )

        return remote_file

    def _sync(self, local_file):
        if self.is_remote(local_file):
            remote_file = self._sync_remote(local_file)
        else:
            remote_file = self._sync_local(local_file)
        return remote_file

    def _upload(self, local_file, remote_file):
        remote_file.copy_from_local(local_file.path)

    def _exists(self, remote_file):
        return remote_file.exists()


class DisabledTaskSyncCtrl(TaskSyncCtrl):
    def _sync(self, local_file):
        return local_file
