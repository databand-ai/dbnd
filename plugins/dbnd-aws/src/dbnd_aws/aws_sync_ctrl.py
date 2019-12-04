from dbnd._core.task_run.task_sync_ctrl import TaskSyncCtrl
from targets.fs import FileSystems


class AwsSyncCtrl(TaskSyncCtrl):
    remote_fs_name = FileSystems.s3

    def _upload(self, local_file, remote_file):
        remote_file.fs.put_multipart(str(local_file), str(remote_file))
