# © Copyright Databand.ai, an IBM Company 2022

import logging

from dbnd_run.task_ctrl.task_sync_ctrl import TaskSyncCtrl


logger = logging.getLogger(__name__)


class AzureDbfsSyncControl(TaskSyncCtrl):
    def __init__(self, mount, task_run):
        super(AzureDbfsSyncControl, self).__init__(task_run=task_run)
        self.mount = mount

    def sync(self, local_file):
        remote_path = super(AzureDbfsSyncControl, self).sync(local_file)
        return self._remote_path_to_dbfs_mount(remote_path)
