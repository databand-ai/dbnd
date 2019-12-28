import logging
import os

from dbnd._core.task_run.task_sync_ctrl import TaskSyncCtrl
from dbnd_azure.fs import AZURE_BLOB_FS_NAME
from targets.fs import FileSystems
from dbnd_azure.fs.azure_blob import AzureBlobStorageClient


logger = logging.getLogger(__name__)


class AzureSyncCtrl(TaskSyncCtrl):
    remote_fs_name = AZURE_BLOB_FS_NAME

    def _upload(self, local_file, remote_file):
        storage_account, container_name, blob_name = remote_file.fs._path_to_account_container_and_blob(
            remote_file
        )

        remote_file.fs.put(local_file, container_name, blob_name)


class AzureDbfsSyncControl(AzureSyncCtrl):
    def __init__(self, mount, task, job):
        super(AzureDbfsSyncControl, self).__init__(task=task, job=job)
        self.mount = mount

    def _remote_path_to_dbfs_mount(self, remote_path):

        storage_account, container_name, blob_name = AzureBlobStorageClient._path_to_account_container_and_blob(
            remote_path
        )
        return "dbfs://%s" % (os.path.join(self.mount, blob_name))

    def sync(self, local_file):
        remote_path = super(AzureDbfsSyncControl, self).sync(local_file)
        return self._remote_path_to_dbfs_mount(remote_path)
