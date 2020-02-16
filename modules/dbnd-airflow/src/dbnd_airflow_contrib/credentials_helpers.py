import logging

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.hooks.base_hook import BaseHook


logger = logging.getLogger(__name__)


class AzureBlobStorageCredentials(BaseHook):
    def __init__(self, conn_id="azure_blob_storage_default"):
        self.conn_id = conn_id

    def get_credentials(self):
        connection_object = self.get_connection(self.conn_id)
        extras = connection_object.extra_dejson
        credentials = dict()

        if connection_object.login:
            credentials["account_name"] = connection_object.login
        if connection_object.password:
            credentials["account_key"] = connection_object.password
        credentials.update(extras)
        return credentials


class GSCredentials(GoogleCloudBaseHook):
    def __init__(self, gcp_conn_id="google_cloud_default", delegate_to=None):
        super(GSCredentials, self).__init__(gcp_conn_id, delegate_to)

    def get_credentials(self):
        try:
            return self._get_credentials()
        except (Exception) as e:
            logger.exception(
                "Failed to load GCP credentials using connection id - "
                + self.gcp_conn_id
            )
            raise


class AwsCredentials(AwsHook):
    def __init__(self, aws_conn_id="aws_default"):
        super(AwsCredentials, self).__init__(aws_conn_id)

    def get_credentials(self, region_name=None):
        return self._get_credentials(region_name)

    def get_s3_resource(self, region_name=None):
        return self.get_resource_type(resource_type="s3", region_name=region_name)
