import logging

from cachetools import cached
from dbnd import Config, parameter
from dbnd._core.plugin.dbnd_plugins import use_airflow_connections
from dbnd_azure.env import AzureCloudConfig


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
        return AzureCloudConfig().simple_params_dict()


class AzureCredentialsConfig(Config):
    _conf__task_family = "azure_credentials"

    account_name = parameter(
        description="""The storage account name. This is used to authenticate requests
                signed with an account key and to construct the storage endpoint. It
                is required unless a connection string is given, or if a custom
                domain is used with anonymous authentication."""
    ).none()[str]
    account_key = parameter(
        description="""The storage account key. This is used for shared key authentication.
                If neither account key or sas token is specified, anonymous access
                will be used.""",
        default=None,
    ).none()[str]
