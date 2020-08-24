import logging
import os

from dbnd._core.task_run.task_sync_ctrl import TaskSyncCtrl
from dbnd_azure.fs import AZURE_BLOB_FS_NAME
from dbnd_azure.fs.azure_blob import AzureBlobStorageClient
from targets.fs import FileSystems


logger = logging.getLogger(__name__)


class AzureDbfsSyncControl(TaskSyncCtrl):
    def __init__(self, mount, task_run):
        super(AzureDbfsSyncControl, self).__init__(task_run=task_run)
        self.mount = mount

    def sync(self, local_file):
        remote_path = super(AzureDbfsSyncControl, self).sync(local_file)
        return self._remote_path_to_dbfs_mount(remote_path)
