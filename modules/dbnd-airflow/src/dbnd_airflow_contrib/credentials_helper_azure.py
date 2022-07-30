# © Copyright Databand.ai, an IBM Company 2022

from airflow.hooks.base_hook import BaseHook


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
