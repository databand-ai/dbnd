import logging

from dbnd._core.task_run.task_sync_ctrl import TaskSyncCtrl
from targets.fs import FileSystems
from targets.utils.path import path_to_bucket_and_key


logger = logging.getLogger(__name__)


class GsSyncCtrl(TaskSyncCtrl):
    remote_fs_name = FileSystems.gcs

    def __init__(self, task, job):
        super(GsSyncCtrl, self).__init__(task=task, job=job)

        # TODO: switch to our target implementation
        gcp_conn_id = self.task_env.conn_id
        from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

        self.storage = GoogleCloudStorageHook(google_cloud_storage_conn_id=gcp_conn_id)

    def _upload(self, local_file, remote_file):
        bucket, key = path_to_bucket_and_key(str(remote_file))
        self.storage.upload(bucket=bucket, object=key, filename=str(local_file))

    def _exists(self, remote_file):
        bucket, key = path_to_bucket_and_key(str(remote_file))
        return self.storage.exists(bucket, key)
