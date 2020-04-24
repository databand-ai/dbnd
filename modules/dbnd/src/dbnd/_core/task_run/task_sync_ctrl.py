import logging

from os import path

import six

from dbnd._core.task_run.task_run_ctrl import TaskRunCtrl
from dbnd._core.utils.task_utils import targets_to_str
from targets import Target, target
from targets.fs import FileSystems


logger = logging.getLogger(__name__)


class TaskSyncCtrl(TaskRunCtrl):
    def __init__(self, task_run):
        super(TaskSyncCtrl, self).__init__(task_run=task_run)

        self.remote_sync_root = self.task_env.dbnd_data_sync_root.folder("deploy")

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

    def remote_file(self, local_file):
        if self.is_remote(local_file):
            return local_file

        file_name = path.basename(local_file.path)
        deploy_id = self.task.settings.output.deploy_id
        remote_file_name = "{}/{}".format(deploy_id, file_name)
        return self.remote_sync_root.partition(remote_file_name)

    def _sync(self, local_file):
        if self.is_remote(local_file):
            return local_file

        remote_file = self.remote_file(local_file)
        if self._exists(remote_file):
            logger.info("File exists: '%s' -> '%s'.", local_file, remote_file)
            return remote_file

        logger.info("Uploading: '%s' -> '%s'", local_file, remote_file)
        self._upload(local_file, remote_file)

        return remote_file

    def _upload(self, local_file, remote_file):
        remote_file.copy_from_local(local_file.path)

    def _exists(self, remote_file):
        return remote_file.exists()


class DisabledTaskSyncCtrl(TaskSyncCtrl):
    def _sync(self, local_file):
        return local_file
