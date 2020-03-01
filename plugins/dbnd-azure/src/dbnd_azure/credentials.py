import logging

from cachetools import cached
from dbnd._core.plugin.dbnd_plugins import use_airflow_connections
from dbnd_azure.env import AzureCredentialsConfig


logger = logging.getLogger(__name__)


@cached(cache={})
def get_azure_credentials():
    if use_airflow_connections():
        from dbnd_airflow_contrib.credentials_helper_azure import (
            AzureBlobStorageCredentials,
        )

        aws_storage_credentials = AzureBlobStorageCredentials()
        logger.debug(
            "getting azure credentials from airflow connection '%s'"
            % aws_storage_credentials.conn_id
        )
        return aws_storage_credentials.get_credentials()
    else:
        logger.debug("getting azure credentials from dbnd config")
        return AzureCredentialsConfig().simple_params_dict()
